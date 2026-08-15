import asyncio
import logging
import random
from datetime import UTC, datetime, timedelta

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage
from sqlalchemy import select, update

from app.broker import (
    DLQ_QUEUE,
    DLX_EXCHANGE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    RETRY_EXCHANGE,
    RETRY_QUEUE,
)
from app.config import get_settings
from app.db import session_factory
from app.models import Payment, PaymentStatus
from app.schemas import PaymentEvent, WebhookPayload
from app.topology import declare_topology

logger = logging.getLogger(__name__)
settings = get_settings()
broker = RabbitBroker(settings.rabbitmq_url)
app = FastStream(broker)


@app.on_startup
async def setup() -> None:
    await declare_topology(settings.rabbitmq_url)


async def process_gateway(payment_id: str) -> None:
    async with session_factory() as session:
        current_status = await session.scalar(
            select(Payment.status).where(Payment.id == payment_id)
        )
    if current_status is None:
        raise ValueError(f"Payment {payment_id} does not exist")
    if current_status != PaymentStatus.PENDING:
        return

    delay = random.uniform(settings.gateway_min_delay, settings.gateway_max_delay)
    await asyncio.sleep(delay)
    new_status = PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED

    async with session_factory() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == payment_id).with_for_update()
        )
        if payment is None:
            raise ValueError(f"Payment {payment_id} does not exist")
        if payment.status == PaymentStatus.PENDING:
            payment.status = new_status
            payment.processed_at = datetime.now(UTC)


async def send_webhook(event: PaymentEvent) -> None:
    payment_id = str(event.payment_id)
    async with session_factory() as session:
        payment = await session.scalar(select(Payment).where(Payment.id == payment_id))
    if payment is None:
        raise ValueError(f"Payment {payment_id} does not exist")
    if payment.status == PaymentStatus.PENDING or payment.processed_at is None:
        raise RuntimeError(f"Payment {payment_id} has not been processed")
    if payment.webhook_delivered_at is not None:
        logger.info("Webhook for payment %s already delivered, skipping", payment_id)
        return

    payload = WebhookPayload(
        payment_id=payment.id,
        status=payment.status,
        amount=payment.amount,
        currency=payment.currency,
        processed_at=payment.processed_at,
    )
    async with httpx.AsyncClient(timeout=settings.webhook_timeout) as client:
        response = await client.post(
            payment.webhook_url,
            json=payload.model_dump(mode="json"),
            headers={
                "X-Payment-Event": "payment.processed",
                "X-Payment-Event-Id": str(event.event_id),
                "Idempotency-Key": str(event.event_id),
            },
        )
        response.raise_for_status()

    async with session_factory() as session, session.begin():
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=datetime.now(UTC))
        )


async def retry_or_dead_letter(
    event: PaymentEvent, message: RabbitMessage, error: Exception
) -> None:
    retry_count = int((message.headers or {}).get("x-retry-count", 0))
    attempt = retry_count + 1
    headers = {"x-retry-count": attempt, "x-last-error": str(error)[:500]}

    if attempt < 3:
        delay = 2 ** (attempt - 1)
        logger.warning(
            "Payment %s failed on attempt %s; retrying in %ss",
            event.payment_id,
            attempt,
            delay,
        )
        await broker.publish(
            event.model_dump(mode="json"),
            queue=RETRY_QUEUE.name,
            exchange=RETRY_EXCHANGE,
            headers=headers,
            persist=True,
            expiration=timedelta(seconds=delay),
            message_id=str(event.event_id),
        )
        return

    logger.error("Payment %s moved to DLQ after %s attempts", event.payment_id, attempt)
    await broker.publish(
        event.model_dump(mode="json"),
        queue=DLQ_QUEUE.name,
        exchange=DLX_EXCHANGE,
        headers=headers,
        persist=True,
        message_id=str(event.event_id),
    )


@broker.subscriber(PAYMENTS_QUEUE, PAYMENTS_EXCHANGE)
async def handle_payment(event: PaymentEvent, message: RabbitMessage) -> None:
    try:
        await process_gateway(str(event.payment_id))
        await send_webhook(event)
    except Exception as exc:
        logger.exception("Payment event %s failed", event.event_id)
        await retry_or_dead_letter(event, message, exc)
