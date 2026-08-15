from decimal import Decimal

import pytest
from pydantic import ValidationError

from app.models import Currency
from app.schemas import PaymentCreate


def valid_payment(**overrides) -> PaymentCreate:
    data = {
        "amount": "1250.50",
        "currency": "RUB",
        "description": "Order 42",
        "metadata": {"order_id": 42},
        "webhook_url": "https://example.com/webhook",
    }
    data.update(overrides)
    return PaymentCreate.model_validate(data)


def test_payment_create_parses_contract() -> None:
    payment = valid_payment()

    assert payment.amount == Decimal("1250.50")
    assert payment.currency is Currency.RUB
    assert payment.metadata == {"order_id": 42}


@pytest.mark.parametrize("amount", ["0", "-1", "1.001"])
def test_payment_create_rejects_invalid_amount(amount: str) -> None:
    with pytest.raises(ValidationError):
        valid_payment(amount=amount)


def test_payment_create_rejects_unknown_currency() -> None:
    with pytest.raises(ValidationError):
        valid_payment(currency="GBP")

