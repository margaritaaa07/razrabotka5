import asyncio
import json
import os
from contextlib import asynccontextmanager

import aio_pika
from aio_pika import ExchangeType
from fastapi import FastAPI, HTTPException

AMQP_URL = os.getenv("AMQP_URL", "amqp://user:password@rabbitmq:5672/")
EXCHANGE = "loyalty.events"
QUEUE = "analytics.events"
QUEUE_ARGS = {"x-queue-type": "quorum"}

# Статистика по магазинам и счётчик событий в памяти
store_stats: dict[str, dict] = {}
events_received = 0


def get_store(store_id: str) -> dict:
    """Возвращает словарь статистики магазина, создавая его при первом обращении."""
    if store_id not in store_stats:
        store_stats[store_id] = {
            "purchase_applied": 0,
            "purchase_rejected": 0,
            "return_applied": 0,
            "return_rejected": 0,
            "redeem_applied": 0,
            "redeem_rejected": 0,
            "points_accrued": 0,
            "points_refunded": 0,
            "points_redeemed": 0,
        }
    return store_stats[store_id]


async def process_event(event: dict) -> None:
    global events_received
    events_received += 1

    store_id = event.get("store_id", "unknown")
    op_type = event.get("type")
    status = event.get("status")
    points_delta = event.get("points_delta", 0)

    stats = get_store(store_id)

    # Счёт количества принятых и отклоненных операций
    if op_type == "purchase":
        stats["purchase_applied" if status == "applied" else "purchase_rejected"] += 1
    elif op_type == "return":
        stats["return_applied" if status == "applied" else "return_rejected"] += 1
    elif op_type == "redeem":
        stats["redeem_applied" if status == "applied" else "redeem_rejected"] += 1

    # Счёт сумм (только для принятых операций)
    if status == "applied":
        if op_type == "purchase":
            stats["points_accrued"] += points_delta
        elif op_type in ("return", "redeem"):
            if op_type == "return":
                stats["points_refunded"] += abs(points_delta)
            else:
                stats["points_redeemed"] += abs(points_delta)

    print(
        f"[ANALYTICS] Событие: магазин {store_id}, операция {event.get('operation_id')}, "
        f"тип {op_type}, статус {status}, delta {points_delta}",
        flush=True,
    )


async def consume_events():
    connection = await aio_pika.connect_robust(AMQP_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    exchange = await channel.declare_exchange(EXCHANGE, ExchangeType.FANOUT, durable=True)
    queue = await channel.declare_queue(QUEUE, durable=True, arguments=QUEUE_ARGS)
    await queue.bind(exchange)

    print("Analytics started, waiting for events...", flush=True)

    async with queue.iterator() as iterator:
        async for message in iterator:
            async with message.process():
                event = json.loads(message.body.decode())
                await process_event(event)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_events())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "analytics"}


@app.get("/stores")
async def get_all_stores():
    """Статистика по всем магазинам. Пустой список, если событий не было."""
    return [{"store_id": sid, **stats} for sid, stats in store_stats.items()]


@app.get("/stores/{store_id}")
async def get_store_stats(store_id: str):
    """Статистика конкретного магазина. 404, если событий не было."""
    if store_id not in store_stats:
        raise HTTPException(status_code=404, detail="store not found")
    return {"store_id": store_id, **store_stats[store_id]}


@app.get("/stats")
async def get_global_stats():
    """Общее число полученных событий."""
    return {"events_received": events_received}