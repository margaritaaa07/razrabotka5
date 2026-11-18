import json
import os
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import select, desc

from db import Session
from models import Account, OperationRecord

AMQP_URL = os.getenv("AMQP_URL", "amqp://user:password@rabbitmq:5672/")

# Основная очередь
EXCHANGE = "loyalty"
QUEUE = "operations"
ROUTING_KEY = "operation"

# Очередь для ошибок (Dead Letter Queue)
DLX_EXCHANGE = "loyalty.dlx"
DLQ_QUEUE = "operations.dlq"
DLX_ROUTING_KEY = "operation.failed"

# Аргументы основной очереди: quorum + привязка к DLX
QUEUE_ARGS = {
    "x-queue-type": "quorum",
    "x-dead-letter-exchange": DLX_EXCHANGE,
    "x-dead-letter-routing-key": DLX_ROUTING_KEY,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    connection = await aio_pika.connect_robust(AMQP_URL)
    channel = await connection.channel(publisher_confirms=True, on_return_raises=True)

    # Основной обменник и очередь
    exchange = await channel.declare_exchange(EXCHANGE, ExchangeType.DIRECT, durable=True)
    queue = await channel.declare_queue(QUEUE, durable=True, arguments=QUEUE_ARGS)
    await queue.bind(exchange, routing_key=ROUTING_KEY)

    # Обменник и очередь для ошибок
    dlx = await channel.declare_exchange(DLX_EXCHANGE, ExchangeType.DIRECT, durable=True)
    dlq = await channel.declare_queue(DLQ_QUEUE, durable=True, arguments={"x-queue-type": "quorum"})
    await dlq.bind(dlx, routing_key=DLX_ROUTING_KEY)

    app.state.connection = connection
    app.state.channel = channel
    app.state.exchange = exchange
    try:
        yield
    finally:
        await connection.close()


app = FastAPI(lifespan=lifespan)


class OperationIn(BaseModel):
    operation_id: str = Field(min_length=1)
    type: str
    customer_id: str = Field(min_length=1)
    store_id: str = Field(min_length=1)
    check_number: str = Field(min_length=1)
    amount: Decimal | None = None
    points: int | None = None
    occurred_at: datetime

    @field_validator("type")
    @classmethod
    def check_type(cls, v):
        if v not in ("purchase", "return", "redeem"):
            raise ValueError("invalid type")
        return v

    @field_validator("amount")
    @classmethod
    def check_amount(cls, v, info):
        t = info.data.get("type")
        if t in ("purchase", "redeem"):
            if v is None or v <= 0:
                raise ValueError("amount must be positive")
        if t == "return" and v is not None:
            raise ValueError("amount must be absent for return")
        return v

    @field_validator("points")
    @classmethod
    def check_points(cls, v, info):
        t = info.data.get("type")
        if t == "redeem":
            if v is None or v <= 0:
                raise ValueError("points must be positive")
        elif v is not None:
            raise ValueError("points must be absent")
        return v


@app.post("/operations", status_code=202)
async def create_operation(op: OperationIn, response: Response):
    payload = {
        "operation_id": op.operation_id,
        "type": op.type,
        "customer_id": op.customer_id,
        "store_id": op.store_id,
        "check_number": op.check_number,
        "amount": str(op.amount) if op.amount is not None else None,
        "points": op.points,
        "occurred_at": op.occurred_at.isoformat(),
    }

    message = Message(
        body=json.dumps(payload).encode(),
        content_type="application/json",
        message_id=op.operation_id,
        delivery_mode=DeliveryMode.PERSISTENT,
    )

    try:
        await app.state.exchange.publish(message, routing_key=ROUTING_KEY, mandatory=True)
    except aio_pika.exceptions.AMQPError:
        response.status_code = 503
        return {"detail": "broker did not confirm publication"}

    return {"operation_id": op.operation_id}


@app.get("/operations/{operation_id}")
async def get_operation(operation_id: str):
    async with Session() as s:
        rec = await s.get(OperationRecord, operation_id)
        if rec is None:
            raise HTTPException(status_code=404, detail="not found")
        return {
            "operation_id": rec.operation_id,
            "type": rec.type,
            "customer_id": rec.customer_id,
            "store_id": rec.store_id,
            "status": rec.status,
            "points_delta": rec.points_delta,
            "balance_after": str(rec.balance_after),
            "reject_reason": rec.reject_reason,
            "processed_at": rec.processed_at.isoformat(),
        }


@app.get("/accounts/{customer_id}")
async def get_account(customer_id: str):
    async with Session() as s:
        acc = await s.get(Account, customer_id)
        if acc is None:
            raise HTTPException(status_code=404, detail="not found")

        rows = (await s.execute(
            select(OperationRecord)
            .where(OperationRecord.customer_id == customer_id)
            .order_by(desc(OperationRecord.processed_at))
            .limit(20)
        )).scalars().all()

        return {
            "customer_id": acc.customer_id,
            "balance": str(acc.balance),
            "operations": [
                {
                    "operation_id": r.operation_id,
                    "type": r.type,
                    "status": r.status,
                    "points_delta": r.points_delta,
                    "balance_after": str(r.balance_after),
                    "reject_reason": r.reject_reason,
                    "processed_at": r.processed_at.isoformat(),
                }
                for r in rows
            ],
        }


@app.get("/health")
async def health(response: Response):
    try:
        async with Session() as s:
            await s.execute(select(1))
    except Exception:
        response.status_code = 503
        return {"status": "unavailable", "db": "down"}

    try:
        if app.state.connection.is_closed:
            response.status_code = 503
            return {"status": "unavailable", "broker": "down"}
    except Exception:
        response.status_code = 503
        return {"status": "unavailable", "broker": "down"}

    return {"status": "ok"}