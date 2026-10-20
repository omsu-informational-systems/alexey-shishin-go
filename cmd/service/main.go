package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/omsu-informational-systems/alexey-shishin/internal/broker"
	"github.com/omsu-informational-systems/alexey-shishin/internal/candles"
	"github.com/omsu-informational-systems/alexey-shishin/internal/config"
	"github.com/omsu-informational-systems/alexey-shishin/internal/events"
	"github.com/omsu-informational-systems/alexey-shishin/internal/health"
	"github.com/omsu-informational-systems/alexey-shishin/internal/httpserver"
	"github.com/omsu-informational-systems/alexey-shishin/internal/registration"
)

const shutdownTimeout = 10 * time.Second

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	creds := broker.Credentials{
		Brokers:  cfg.KafkaBootstrapServers,
		Username: cfg.KafkaUsername,
		Password: cfg.KafkaPassword,
	}

	dialer, err := broker.NewDialer(creds)
	if err != nil {
		log.Fatalf("broker dialer setup failed: %v", err)
	}
	transport, err := broker.NewTransport(creds)
	if err != nil {
		log.Fatalf("broker transport setup failed: %v", err)
	}

	heartbeatLiveness := broker.NewLivenessChecker("heartbeat-producer")
	registrationReaderLiveness := broker.NewLivenessChecker("registration-reader")
	candleReaderLiveness := broker.NewLivenessChecker("candle-reader")
	registrationHealth := registration.NewHealthChecker()

	registry := health.NewRegistry(registrationHealth, heartbeatLiveness, registrationReaderLiveness, candleReaderLiveness)
	srv := httpserver.New(":"+cfg.Port, cfg.ServiceName, cfg.ServiceVersion, registry)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("%s listening on :%s", cfg.ServiceName, cfg.Port)
		serveErr <- srv.ListenAndServe()
	}()

	producer := broker.NewKafkaProducer(broker.ProducerParams{
		Brokers:   cfg.KafkaBootstrapServers,
		Transport: transport,
		Logger:    logger,
		Liveness:  heartbeatLiveness,
	})

	registrationReader, err := broker.NewKafkaConsumer(ctx, broker.ConsumerParams{
		Brokers:  cfg.KafkaBootstrapServers,
		Topic:    cfg.TopicRegistrationResult,
		GroupID:  broker.GroupID(cfg.KafkaGroupID, "registration"),
		Dialer:   dialer,
		Logger:   logger,
		Liveness: registrationReaderLiveness,
	})
	if err != nil {
		log.Fatalf("registration result reader setup failed: %v", err)
	}

	identity := registration.Identity{
		SecretKey:    cfg.AccountSecretKey,
		AccountID:    cfg.AccountID,
		AccountName:  cfg.AccountName,
		GroupID:      cfg.GroupID,
		StrategyID:   cfg.StrategyID,
		StrategyName: cfg.StrategyName,
		ServiceName:  cfg.ServiceName,
		Description:  cfg.Description,
	}

	flow := &registration.Flow{
		Producer:     producer,
		Consumer:     registrationReader,
		Identity:     identity,
		RequestTopic: cfg.TopicRegistrationRequest,
		ResultTopic:  cfg.TopicRegistrationResult,
		Health:       registrationHealth,
		Logger:       logger,
	}

	instanceID := events.NewUUIDv4()
	startedAt := time.Now().UTC()

	var candleReaderMu sync.Mutex
	var candleReader broker.Consumer

	go func() {
		result, flowErr := flow.Run(ctx)
		if flowErr != nil {
			if ctx.Err() == nil {
				logger.Error("registration flow failed", "error", flowErr.Error())
			}
			return
		}

		if result.Status != events.RegistrationStatusRegistered {
			logger.Error("registration rejected by platform", "reason", result.Reason)
			return
		}

		logger.Info("registration succeeded", "accountId", identity.AccountID)

		reader, candleErr := broker.NewKafkaConsumer(ctx, broker.ConsumerParams{
			Brokers:  cfg.KafkaBootstrapServers,
			Topic:    cfg.CandleTopic,
			GroupID:  broker.GroupID(cfg.KafkaGroupID, "candles"),
			Dialer:   dialer,
			Logger:   logger,
			Liveness: candleReaderLiveness,
		})
		if candleErr != nil {
			logger.Error("candle reader setup failed", "error", candleErr.Error())
			return
		}

		candleReaderMu.Lock()
		candleReader = reader
		candleReaderMu.Unlock()

		candleConsumer := candles.New(reader, logger)

		heartbeat := &registration.Heartbeat{
			Producer:   producer,
			Topic:      cfg.TopicHeartbeat,
			Identity:   identity,
			InstanceID: instanceID,
			StartedAt:  startedAt,
			Counters:   candleConsumer,
			Logger:     logger,
		}

		go heartbeat.Run(ctx)
		candleConsumer.Run(ctx)
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed: %v", err)
		}
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining connections")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
			os.Exit(1)
		}
		log.Printf("shutdown complete")
	}

	if err := producer.Close(); err != nil {
		log.Printf("producer close failed: %v", err)
	}
	if err := registrationReader.Close(); err != nil {
		log.Printf("registration reader close failed: %v", err)
	}
	candleReaderMu.Lock()
	if candleReader != nil {
		if err := candleReader.Close(); err != nil {
			log.Printf("candle reader close failed: %v", err)
		}
	}
	candleReaderMu.Unlock()

	os.Exit(0)
}
