# vwap-reversion

Сервис торговой стратегии (возврат к VWAP) для учебной практики
"Проектирование информационных систем" (платформа `omsu.7bits.it`). Задание 1:
каркас серверного приложения на Go stdlib и эндпоинт проверки состояния
`GET /health`; интеграция с платформой и сама стратегия — в последующих
заданиях.

**Автор:** Шишин Алексей Олегович
**Проектный документ:** [docs/project-description.md](docs/project-description.md)

## Требования к окружению

- Go 1.22+ — для сборки без Docker.
- Docker — для сборки и запуска в контейнере.
- Внешних зависимостей нет, используется только stdlib.

## Переменные окружения

| Переменная | Назначение | Дефолт |
|---|---|---|
| `PORT` | TCP-порт HTTP-сервера | `8080` |
| `SERVICE_NAME` | Имя сервиса в ответе `/health` | `vwap-reversion` |
| `SERVICE_VERSION` | Версия (SemVer) в ответе `/health` | `0.1.0` |

Пример значений — в `.env.example`. Секреты в Git не хранятся.

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

Остановка (graceful shutdown по SIGTERM): `docker stop <container_id>`.

## Проверка состояния

```
curl -i localhost:8080/health
```

```json
{
  "status": "UP",
  "service": "vwap-reversion",
  "version": "0.1.0",
  "components": {}
}
```

Доступен без заголовка `Authorization`.

## Тесты

```
go test ./...
```
