FROM golang:1.26-alpine AS builder

ARG SERVICE_VERSION=0.1.0
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/service ./cmd/service

FROM alpine:3.20 AS runtime

ARG SERVICE_PORT=8080
ARG SERVICE_VERSION=0.1.0

RUN apk add --no-cache ca-certificates && \
    addgroup -S app && adduser -S app -G app

ENV PORT=${SERVICE_PORT} \
    SERVICE_VERSION=${SERVICE_VERSION}

COPY --from=builder /out/service /usr/local/bin/service

USER app
EXPOSE ${SERVICE_PORT}

ENTRYPOINT ["/usr/local/bin/service"]
