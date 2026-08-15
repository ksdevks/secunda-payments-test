from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.exc import IntegrityError

from app.models import Currency, Payment
from app.payment_service import _same_request, create_payment
from app.schemas import PaymentCreate


def request(amount: str = "10.00") -> PaymentCreate:
    return PaymentCreate(
        amount=Decimal(amount),
        currency=Currency.USD,
        description="Test",
        metadata={"invoice": 7},
        webhook_url="https://example.com/hook",
    )


def payment() -> Payment:
    return Payment(
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="Test",
        metadata_={"invoice": 7},
        idempotency_key="key",
        webhook_url="https://example.com/hook",
    )


def test_idempotent_request_must_match_original() -> None:
    assert _same_request(payment(), request())
    assert not _same_request(payment(), request("11.00"))


class TransactionContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


async def test_unrelated_integrity_error_is_preserved() -> None:
    original_error = IntegrityError("INSERT", {}, ValueError("different constraint"))
    session = Mock()
    session.begin.return_value = TransactionContext()
    session.flush = AsyncMock(side_effect=original_error)
    session.scalar = AsyncMock(return_value=None)

    with pytest.raises(IntegrityError) as raised:
        await create_payment(session, request(), "new-key")

    assert raised.value is original_error
    session.scalar.assert_awaited_once()
