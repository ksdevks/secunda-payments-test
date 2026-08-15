import asyncio
import logging
from contextlib import suppress
from datetime import UTC, datetime, timedelta

from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from app.broker import PAYMENTS_EXCHANGE
from app.config import get_settings
from app.db import session_factory
from app.models import OutboxEvent

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(self, broker: RabbitBroker) -> None:
        self.broker = broker
        self.settings = get_settings()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="outbox-relay")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task

    async def _run(self) -> None:
        while True:
            try:
                published = await self._publish_batch()
            except Exception:
                logger.exception("Outbox relay batch failed unexpectedly")
                published = False
            if not published:
                await asyncio.sleep(self.settings.outbox_poll_interval)

    async def _publish_batch(self) -> bool:
        now = datetime.now(UTC)
        async with session_factory() as session, session.begin():
            events = list(
                (
                    await session.scalars(
                        select(OutboxEvent)
                        .where(
                            OutboxEvent.published_at.is_(None),
                            OutboxEvent.next_attempt_at <= now,
                        )
                        .order_by(OutboxEvent.created_at)
                        .limit(50)
                        .with_for_update(skip_locked=True)
                    )
                ).all()
            )
            for event in events:
                try:
                    await self.broker.publish(
                        event.payload,
                        queue=event.topic,
                        exchange=PAYMENTS_EXCHANGE,
                        persist=True,
                        message_id=str(event.id),
                    )
                    event.published_at = datetime.now(UTC)
                    event.last_error = None
                except Exception as exc:  # relay must survive broker outages
                    event.attempts += 1
                    event.last_error = str(exc)[:2000]
                    delay = min(2 ** min(event.attempts, 8), 300)
                    event.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
                    logger.exception("Failed to publish outbox event %s", event.id)
            return bool(events)
