import asyncio
import json
import os
from datetime import datetime
from decimal import Decimal

import aio_pika
from aio_pika import DeliveryMode, ExchangeType, Message
from sqlalchemy import select

from db import Session, engine
from domain import decide_purchase, decide_return, decide_redeem, Decision
from models import Account, Base, OperationRecord

AMQP_URL = os.getenv("AMQP_URL", "amqp://user:password@rabbitmq:5672/")

# Основная очередь операций
EXCHANGE = "loyalty"
QUEUE = "operations"
ROUTING_KEY = "operation"

# Очередь ошибок (DLQ)
DLX_EXCHANGE = "loyalty.dlx"
DLQ_QUEUE = "operations.dlq"
DLX_ROUTING_KEY = "operation.failed"

QUEUE_ARGS = {
    "x-queue-type": "quorum",
    "x-dead-letter-exchange": DLX_EXCHANGE,
    "x-dead-letter-routing-key": DLX_ROUTING_KEY,
}

# Обменник для широковещательной рассылки событий
EVENTS_EXCHANGE = "loyalty.events"


async def init_db() -> None:
    """Таблицы создаются автоматически при запуске worker."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def process_operation(payload: dict, events_exchange: aio_pika.Exchange) -> None:
    op_id = payload["operation_id"]

    async with Session() as session:
        async with session.begin():
            # Идемпотентность: если операция уже есть — пропускаем
            existing = await session.get(OperationRecord, op_id)
            if existing is not None:
                return

            customer_id = payload["customer_id"]
            op_type = payload["type"]
            check_number = payload["check_number"]
            amount = Decimal(str(payload["amount"])) if payload.get("amount") is not None else None
            points = payload.get("points")

            # Счёт покупателя: создаём с нулевым балансом при первой операции
            acc = await session.get(Account, customer_id)
            if acc is None:
                acc = Account(customer_id=customer_id, balance=Decimal("0"))
                session.add(acc)
                await session.flush()

            # Расчёт по типу операции
            if op_type == "purchase":
                dup = await session.scalar(
                    select(OperationRecord.operation_id).where(
                        OperationRecord.customer_id == customer_id,
                        OperationRecord.check_number == check_number,
                        OperationRecord.type == "purchase",
                        OperationRecord.status == "applied",
                    )
                )
                decision = decide_purchase(amount, duplicate=dup is not None)

            elif op_type == "return":
                purchase = await session.scalar(
                    select(OperationRecord).where(
                        OperationRecord.customer_id == customer_id,
                        OperationRecord.check_number == check_number,
                        OperationRecord.type == "purchase",
                        OperationRecord.status == "applied",
                    )
                )
                already_returned = await session.scalar(
                    select(OperationRecord.operation_id).where(
                        OperationRecord.customer_id == customer_id,
                        OperationRecord.check_number == check_number,
                        OperationRecord.type == "return",
                        OperationRecord.status == "applied",
                    )
                )
                decision = decide_return(
                    purchase_amount=purchase.amount if purchase else None,
                    purchase_customer=purchase.customer_id if purchase else None,
                    customer_id=customer_id,
                    already_returned=already_returned is not None,
                )

            elif op_type == "redeem":
                decision = decide_redeem(amount, int(points), acc.balance)

            else:
                raise ValueError(f"unknown operation type: {op_type}")

            # Обновляем баланс и сохраняем операцию
            acc.balance = acc.balance + Decimal(decision.points_delta)

            session.add(OperationRecord(
                operation_id=op_id,
                type=op_type,
                customer_id=customer_id,
                store_id=payload["store_id"],
                check_number=check_number,
                amount=amount,
                points=points,
                points_delta=decision.points_delta,
                balance_after=acc.balance,
                status=decision.status,
                reject_reason=decision.reject_reason,
                occurred_at=datetime.fromisoformat(payload["occurred_at"]),
            ))

            # Публикация события после успешной обработки (внутри транзакции)
            event_payload = {
                "operation_id": op_id,
                "type": op_type,
                "customer_id": customer_id,
                "store_id": payload["store_id"],
                "status": decision.status,
                "points_delta": decision.points_delta,
                "balance_after": str(acc.balance),
                "reject_reason": decision.reject_reason,
            }
            await events_exchange.publish(
                Message(
                    body=json.dumps(event_payload).encode(),
                    content_type="application/json",
                    message_id=op_id,
                    delivery_mode=DeliveryMode.PERSISTENT,
                ),
                routing_key="",  # fanout игнорирует routing key
            )


async def main() -> None:
    await init_db()

    connection = await aio_pika.connect_robust(AMQP_URL)
    try:
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        await channel.set_qos(prefetch_count=1)

        # Объявление основной очереди и DLQ
        exchange = await channel.declare_exchange(EXCHANGE, ExchangeType.DIRECT, durable=True)
        queue = await channel.declare_queue(QUEUE, durable=True, arguments=QUEUE_ARGS)
        await queue.bind(exchange, routing_key=ROUTING_KEY)

        dlx = await channel.declare_exchange(DLX_EXCHANGE, ExchangeType.DIRECT, durable=True)
        dlq = await channel.declare_queue(DLQ_QUEUE, durable=True, arguments={"x-queue-type": "quorum"})
        await dlq.bind(dlx, routing_key=DLX_ROUTING_KEY)

        # Объявление обменника событий и очередей подписчиков (ДО начала обработки операций)
        events_exchange = await channel.declare_exchange(
            EVENTS_EXCHANGE, ExchangeType.FANOUT, durable=True
        )

        notifier_q = await channel.declare_queue(
            "notifier.events", durable=True, arguments={"x-queue-type": "quorum"}
        )
        await notifier_q.bind(events_exchange)

        analytics_q = await channel.declare_queue(
            "analytics.events", durable=True, arguments={"x-queue-type": "quorum"}
        )
        await analytics_q.bind(events_exchange)

        print("Worker started, waiting for messages...", flush=True)

        # Цикл обработки сообщений
        async with queue.iterator() as iterator:
            async for message in iterator:
                try:
                    async with message.process(requeue=False):
                        payload = json.loads(message.body.decode())
                        await process_operation(payload, events_exchange)
                except Exception as e:
                    print(f"Error processing message: {e}. Message sent to DLQ.", flush=True)
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())