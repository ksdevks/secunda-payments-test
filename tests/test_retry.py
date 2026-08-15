from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from app import consumer
from app.broker import RETRY_EXCHANGE, RETRY_QUEUE
from app.schemas import PaymentEvent


async def test_retry_is_published_to_ttl_queue_without_sleep(monkeypatch) -> None:
    publish = AsyncMock()
    sleep = AsyncMock()
    monkeypatch.setattr(consumer.broker, "publish", publish)
    monkeypatch.setattr(consumer.asyncio, "sleep", sleep)
    event = PaymentEvent(event_id=uuid4(), payment_id=uuid4())
    message = SimpleNamespace(headers={})

    await consumer.retry_or_dead_letter(event, message, RuntimeError("temporary"))

    sleep.assert_not_awaited()
    publish.assert_awaited_once()
    kwargs = publish.await_args.kwargs
    assert kwargs["queue"] == RETRY_QUEUE.name
    assert kwargs["exchange"] == RETRY_EXCHANGE
    assert kwargs["expiration"] == timedelta(seconds=1)
    assert kwargs["headers"]["x-retry-count"] == 1
