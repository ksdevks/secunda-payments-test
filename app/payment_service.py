import uuid

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetails


def _same_request(payment: Payment, request: PaymentCreate) -> bool:
    return (
        payment.amount == request.amount
        and payment.currency == request.currency
        and payment.description == request.description
        and payment.metadata_ == request.metadata
        and payment.webhook_url == str(request.webhook_url)
    )


def to_accepted(payment: Payment) -> PaymentAccepted:
    return PaymentAccepted(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


def to_details(payment: Payment) -> PaymentDetails:
    return PaymentDetails(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        status=payment.status,
        idempotency_key=payment.idempotency_key,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )


async def create_payment(
    session: AsyncSession, request: PaymentCreate, idempotency_key: str
) -> Payment:
    payment_id = uuid.uuid4()
    payment = Payment(
        id=payment_id,
        amount=request.amount,
        currency=request.currency,
        description=request.description,
        metadata_=request.metadata,
        idempotency_key=idempotency_key,
        webhook_url=str(request.webhook_url),
    )
    event_id = uuid.uuid4()
    event = OutboxEvent(
        id=event_id,
        topic="payments.new",
        payload={"event_id": str(event_id), "payment_id": str(payment_id)},
    )

    try:
        async with session.begin():
            session.add(payment)
            session.add(event)
            await session.flush()
        return payment
    except IntegrityError:
        # session.begin() has already rolled the failed transaction back here.
        existing = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if existing is None:
            # The integrity error was unrelated to the idempotency constraint.
            raise
        if not _same_request(existing, request):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency-Key was already used with a different request",
            ) from None
        return existing


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return payment
