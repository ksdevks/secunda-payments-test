# Payment Service

Минимальный микросервис асинхронной обработки платежей на FastAPI, FastStream,
PostgreSQL и RabbitMQ.

## Как это работает

1. `POST /api/v1/payments` в одной транзакции создаёт `payments` и событие в `outbox`.
2. Outbox relay в процессе API блокирует непубликованные строки через
   `FOR UPDATE SKIP LOCKED`, публикует persistent-сообщение в `payments.new` и только
   после подтверждения RabbitMQ отмечает событие опубликованным.
3. Один consumer ждёт 2–5 секунд, выбирает результат (90% `succeeded`, 10% `failed`),
   фиксирует его в PostgreSQL и отправляет webhook.
4. Технические ошибки обработки или webhook повторяются до трёх попыток. Сообщения
   проходят через TTL-очередь `payments.retry` с задержками 1 и 2 секунды, не занимая
   consumer. После третьей попытки событие попадает в `payments.failed` через exchange
   `payments.dlx`.

Outbox даёт доставку **at least once**: при сбое между publish confirm и commit событие
может быть опубликовано повторно. Consumer учитывает это — уже завершённый платёж не
обрабатывается шлюзом повторно. После подтверждённой доставки webhook сохраняется
`webhook_delivered_at`, поэтому последующие доставки сообщения пропускают уведомление.

Между успешным HTTP-ответом и сохранением маркера остаётся неизбежное окно сбоя. Для
дедупликации на стороне получателя webhook содержит одинаковые для всех повторов
`Idempotency-Key` и `X-Payment-Event-Id`. Гарантия HTTP-доставки остаётся at least once.

## Запуск

Нужны Docker и Docker Compose.

```bash
cp .env.example .env
docker compose up --build
```

Значение `dev-secret` предназначено только для локального запуска. В production
`API_KEY` должен обязательно передаваться через secret storage без fallback-значения.

После запуска:

- API: <http://localhost:8000/docs>
- RabbitMQ UI: <http://localhost:15672> (`payments` / `payments`)

Миграция применяется автоматически перед запуском API. Остановить окружение:

```bash
docker compose down
```

Удалить также данные PostgreSQL:

```bash
docker compose down -v
```

## Примеры API

Создание платежа:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: dev-secret' \
  -H 'Idempotency-Key: order-42-attempt-1' \
  -d '{
    "amount": "1250.50",
    "currency": "RUB",
    "description": "Оплата заказа 42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://example.com/payment-webhook"
  }'
```

Ответ имеет код `202 Accepted`:

```json
{
  "payment_id": "a655e572-ae9f-4770-ab18-ebc9ad01c741",
  "status": "pending",
  "created_at": "2026-08-12T10:00:00Z"
}
```

Повтор идентичного запроса с тем же `Idempotency-Key` вернёт тот же платёж. Если тело
изменено, API вернёт `409 Conflict`.

Получение платежа:

```bash
curl http://localhost:8000/api/v1/payments/a655e572-ae9f-4770-ab18-ebc9ad01c741 \
  -H 'X-API-Key: dev-secret'
```

Webhook имеет заголовки `X-Payment-Event: payment.processed`, `X-Payment-Event-Id` и
`Idempotency-Key`, а также тело:

```json
{
  "payment_id": "a655e572-ae9f-4770-ab18-ebc9ad01c741",
  "status": "succeeded",
  "amount": "1250.50",
  "currency": "RUB",
  "processed_at": "2026-08-12T10:00:04Z"
}
```

## Локальная разработка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
ruff check .
pytest
```

Основные настройки перечислены в `.env.example`. Для быстрых тестов можно задать
`GATEWAY_MIN_DELAY=0` и `GATEWAY_MAX_DELAY=0`.

## Структура

```text
app/api.py              HTTP API и lifespan outbox relay
app/payment_service.py  транзакции создания/чтения и idempotency
app/outbox.py           публикация transactional outbox
app/consumer.py         шлюз, обновление БД, webhook, retry и DLQ
app/topology.py         exchange/queue/binding RabbitMQ
app/models.py           SQLAlchemy-модели
alembic/                миграции
docker-compose.yml      postgres, rabbitmq, api, consumer
```
