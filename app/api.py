import uuid
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, status
from faststream.rabbit import RabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_api_key
from app.config import get_settings
from app.db import get_session
from app.outbox import OutboxRelay
from app.payment_service import create_payment, get_payment, to_accepted, to_details
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetails
from app.topology import declare_topology

settings = get_settings()
broker = RabbitBroker(settings.rabbitmq_url)
relay = OutboxRelay(broker)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await declare_topology(settings.rabbitmq_url)
    await broker.start()
    relay.start()
    try:
        yield
    finally:
        await relay.stop()
        await broker.stop()


app = FastAPI(
    title="Payment Service",
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(require_api_key)],
)


@app.post(
    "/api/v1/payments",
    response_model=PaymentAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def post_payment(
    request: PaymentCreate,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentAccepted:
    payment = await create_payment(session, request, idempotency_key)
    return to_accepted(payment)


@app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetails)
async def read_payment(
    payment_id: uuid.UUID,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> PaymentDetails:
    payment = await get_payment(session, payment_id)
    return to_details(payment)


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}
