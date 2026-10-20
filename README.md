# vwap-reversion

Сервис торговой стратегии (возврат к VWAP) для учебной практики
"Проектирование информационных систем" (платформа `omsu.7bits.it`). 

- Задание 1: каркас серверного приложения на Go stdlib и эндпоинт проверки состояния `GET /health`. 
- Задание 2: Kafka-интеграция — регистрация сервиса на  
платформе, heartbeat, приём рыночных свечей `market.candle.closed`. 

**Автор:** Шишин Алексей Олегович
  
**Проектный документ:** [docs/project-description.md](docs/project-description.md)
  
**Трекер задач:** [GitHub Issues](https://github.com/omsu-informational-systems/alexey-shishin-go/issues)

## Требования к окружению

- Go 1.23+ — для сборки без Docker.
- Docker — для сборки и запуска в контейнере.
- Единственная внешняя зависимость — `github.com/segmentio/kafka-go` (pure
Go, без cgo/librdkafka).
- Для подключения к Kafka нужны реальные учётные данные, выданные
преподавателем (см. таблицу переменных ниже) — без них сервис не
запускается (`config.Load()` вернёт ошибку с именем отсутствующей
переменной).

## Переменные окружения


| Переменная                   | Назначение                                                                                        | Дефолт                                |
| ---------------------------- | ------------------------------------------------------------------------------------------------- | ------------------------------------- |
| `PORT`                       | TCP-порт HTTP-сервера                                                                             | `8080`                                |
| `SERVICE_NAME`               | Имя сервиса в ответе `/health` и в событиях регистрации                                           | `vwap-reversion`                      |
| `SERVICE_VERSION`            | Версия (SemVer) в ответе `/health`                                                                | `0.1.0`                               |
| `KAFKA_BOOTSTRAP_SERVERS`    | Адрес(а) брокера, через запятую                                                                   | — (обязательна, секрет платформы)     |
| `KAFKA_USERNAME`             | Логин SASL\_SSL/SCRAM-SHA-256                                                                     | — (обязательна, секрет)               |
| `KAFKA_PASSWORD`             | Пароль SASL\_SSL/SCRAM-SHA-256                                                                    | — (обязательна, секрет)               |
| `KAFKA_GROUP_ID`             | Базовый consumer-group id; код добавляет суффиксы `.registration`/`.candles` для разных читателей | — (обязательна)                       |
| `CANDLE_TOPIC`               | Топик со свечами `market.candle.closed`                                                           | — (обязательна, выдаёт преподаватель) |
| `TOPIC_REGISTRATION_REQUEST` | Топик запроса регистрации                                                                         | `account.registration.request`        |
| `TOPIC_REGISTRATION_RESULT`  | Топик результата регистрации                                                                      | `account.registration.result`         |
| `TOPIC_HEARTBEAT`            | Топик heartbeat                                                                                   | `account.service.heartbeat`           |
| `ACCOUNT_SECRET_KEY`         | Секрет аккаунта для регистрации                                                                   | — (обязательна, секрет)               |
| `ACCOUNT_ID`                 | Идентификатор аккаунта                                                                            | — (обязательна)                       |
| `ACCOUNT_NAME`               | Имя аккаунта                                                                                      | — (обязательна)                       |
| `GROUP_ID`                   | Учебная группа (выдаёт преподаватель)                                                             | — (обязательна)                       |
| `STRATEGY_ID`                | Идентификатор стратегии                                                                           | — (обязательна)                       |
| `STRATEGY_NAME`              | Имя стратегии                                                                                     | — (обязательна)                       |
| `DESCRIPTION`                | Необязательное описание в запросе регистрации                                                     | \`\` (пусто)                          |


Пример (имена переменных без секретных значений) — в `.env.example`. Секреты
в Git не хранятся.

## Сборка и запуск без IDE

Локально:

```
go build ./cmd/service
./service
```

В Docker:

```
docker build -t vwap-reversion .
docker run --rm -p 8080:8080 vwap-reversion
```

Остановка (graceful shutdown по SIGTERM): `docker stop [[ORCA_RICH_MD:f30fbdbe72eb254c0a4123d6a6182d2b:inline-html:%3Ccontainer_id%3E]]`.

## Проверка состояния

```
curl -i localhost:8080/health
```

```json
{
  "status": "UP",
  "service": "vwap-reversion",
  "version": "0.1.0",
  "components": {
    "registration": {"status": "UP"},
    "heartbeat-producer": {"status": "UP"},
    "registration-reader": {"status": "UP"},
    "candle-reader": {"status": "UP"}
  }
}
```

Компонент `registration` отражает исход регистрации на платформе
(`UNKNOWN` до ответа, `UP` после `REGISTERED`, `DOWN` с `details=reason`
после `REJECTED`). `heartbeat-producer`/`registration-reader`/
`candle-reader` — отдельные чекеры транспортного уровня Kafka: здоровый
heartbeat не маскирует сломанный приём свечей, у каждого канала свой статус.
`/health` доступен без заголовка `Authorization`.

## Логи

Приём свечей и работа Kafka-клиента логируются структурно через `log/slog`
(JSON). В лог попадают только технические поля (`eventId`, `correlationId`,
`runId`, `ticker`, `sequenceNumber` и т.п.) — ни полный payload события, ни
`secretKey`/пароль Kafka в лог не пишутся.

## Тесты

```
go test ./...
```

Kafka-зависимые пакеты (`internal/broker`, `internal/registration`,
`internal/candles`) тестируются через fake Producer/Consumer — без
обращения к реальному брокеру.

## Схемы

Контекстная и компонентная диаграммы — в `docs/project-description.md`
(раздел "Схемы"), исходники и `.svg` — в `docs/diagrams/`.