"""
Pydantic models for Transaction, Order, and Chargeback data validation.
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic.alias_generators import to_camel


class Currency(str, Enum):
    """Supported currencies."""
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    ILS = "ILS"


class TransactionStatus(str, Enum):
    """Transaction status values."""
    COMPLETED = "completed"
    FAILED = "failed"
    PENDING = "pending"


class PaymentMethodType(str, Enum):
    """Payment method types."""
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    WALLET = "wallet"


class PaymentStatus(str, Enum):
    """Order payment status values."""
    PAID = "paid"
    FAILED = "failed"
    REFUNDED = "refunded"


class ChargebackStatus(str, Enum):
    """Chargeback status values."""
    OPEN = "open"
    WON = "won"
    LOST = "lost"
    PENDING_RESPONSE = "pending_response"


class PaymentMethod(BaseModel):
    """Payment method details."""
    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=to_camel,
    )

    type: PaymentMethodType
    provider: str


class OrderItem(BaseModel):
    """Individual item within an order."""
    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=to_camel,
    )

    product_id: str
    quantity: int = Field(gt=0)
    unit_price: float

    @field_validator("unit_price")
    @classmethod
    def validate_unit_price_positive(cls, v: float) -> float:
        """Ensure unit price is strictly positive."""
        if v <= 0:
            raise ValueError("unit_price must be strictly positive")
        return v


class Order(BaseModel):
    """Order model with validation."""
    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=to_camel,
    )

    order_id: str
    customer_id: str
    timestamp: datetime
    total_amount: float
    currency: Currency
    items: List[OrderItem]
    payment_status: PaymentStatus

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Union[str, datetime]) -> datetime:
        """Parse timestamp string to datetime object."""
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(v)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid timestamp format: {v}") from e

    @field_validator("total_amount")
    @classmethod
    def validate_total_amount_positive(cls, v: float) -> float:
        """Ensure total amount is strictly positive."""
        if v <= 0:
            raise ValueError("total_amount must be strictly positive")
        return v

    def validate_currency(self) -> bool:
        """Check if the currency is in the supported list."""
        supported = {Currency.USD, Currency.EUR, Currency.GBP, Currency.ILS}
        return self.currency in supported


class Transaction(BaseModel):
    """Transaction model with validation."""
    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=to_camel,
    )

    transaction_id: str
    order_id: str
    timestamp: datetime
    amount: float
    currency: Currency
    status: TransactionStatus
    payment_method: PaymentMethod
    error_code: Optional[str] = None

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: Union[str, datetime]) -> datetime:
        """Parse timestamp string to datetime object."""
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(v)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid timestamp format: {v}") from e

    @field_validator("amount")
    @classmethod
    def validate_amount_positive(cls, v: float) -> float:
        """Ensure amount is strictly positive."""
        if v <= 0:
            raise ValueError("amount must be strictly positive")
        return v

    @model_validator(mode="after")
    def validate_error_code_consistency(self) -> "Transaction":
        """Ensure error_code is set only for failed transactions."""
        if self.status == TransactionStatus.FAILED and self.error_code is None:
            raise ValueError("error_code is required for failed transactions")
        if self.status != TransactionStatus.FAILED and self.error_code is not None:
            raise ValueError("error_code should be null for non-failed transactions")
        return self

    def validate_currency(self) -> bool:
        """Check if the currency is in the supported list."""
        supported = {Currency.USD, Currency.EUR, Currency.GBP, Currency.ILS}
        return self.currency in supported


class Chargeback(BaseModel):
    """Chargeback model with validation."""
    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=to_camel,
    )

    transaction_id: str
    dispute_date: datetime
    amount: float
    reason_code: str
    status: ChargebackStatus
    resolution_date: Optional[datetime] = None

    @field_validator("dispute_date", "resolution_date", mode="before")
    @classmethod
    def parse_dates(cls, v: Optional[Union[str, datetime]]) -> Optional[datetime]:
        """Parse date strings to datetime objects."""
        if v is None or v == "":
            return None
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(v)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid date format: {v}") from e

    @field_validator("amount")
    @classmethod
    def validate_amount_positive(cls, v: float) -> float:
        """Ensure amount is strictly positive."""
        if v <= 0:
            raise ValueError("amount must be strictly positive")
        return v

    @model_validator(mode="after")
    def validate_resolution_date_consistency(self) -> "Chargeback":
        """Ensure resolution_date is set only for resolved chargebacks."""
        resolved_statuses = {ChargebackStatus.WON, ChargebackStatus.LOST}
        if self.status in resolved_statuses and self.resolution_date is None:
            raise ValueError("resolution_date is required for won/lost chargebacks")
        return self
