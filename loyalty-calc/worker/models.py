from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import DateTime, Integer, Numeric, String, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Account(Base):
    __tablename__ = "accounts"

    customer_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    balance: Mapped[Decimal] = mapped_column(
        Numeric(20, 2), nullable=False, default=Decimal("0")
    )


class OperationRecord(Base):
    __tablename__ = "operations"
    __table_args__ = (
        # номер чека уникален только в рамках клиента
        UniqueConstraint("customer_id", "check_number", name="uq_customer_check"),
    )

    operation_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    type: Mapped[str] = mapped_column(String(16), nullable=False)
    customer_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    store_id: Mapped[str] = mapped_column(String(32), nullable=False)
    check_number: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    amount: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 2), nullable=True)
    points: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    points_delta: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    balance_after: Mapped[Decimal] = mapped_column(
        Numeric(20, 2), nullable=False, default=Decimal("0")
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    reject_reason: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )