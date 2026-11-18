import asyncio
import json
import os
import random
from contextlib import asynccontextmanager

import aio_pika
from aio_pika import ExchangeType
from fastapi import FastAPI, Query

AMQP_URL = os.getenv("AMQP_URL", "amqp://user:password@rabbitmq:5672/")
EXCHANGE = "loyalty.events"
QUEUE = "notifier.events"
QUEUE_ARGS = {"x-queue-type": "quorum"}

MIN_DELAY_MS = int(os.getenv("MIN_DELAY_MS", "100"))
MAX_DELAY_MS = int(os.getenv("MAX_DELAY_MS", "1000"))

UNSUBSCRIBED = {"C999", "C777"}

notifications_store: dict[str, list[dict]] = {}
stats = {"events_received": 0, "notifications_sent": 0}


def build_message(event: dict) -> str:
    if event["status"] == "rejected":
        return f"Операция отклонена: {event.get('reject_reason')}"
    delta = event["points_delta"]
    if delta >= 0:
        return f"Начислено {delta} баллов, баланс {event['balance_after']}"
    return f"Списано {abs(delta)} баллов, баланс {event['balance_after']}"


async def process_event(event: dict) -> None:
    stats["events_received"] += 1

    customer_id = event["customer_id"]
    if customer_id in UNSUBSCRIBED:
        print(f"[NOTIFIER] {customer_id} отписан, уведомление не отправлено", flush=True)
        return

    text = build_message(event)
    delay_ms = random.randint(MIN_DELAY_MS, MAX_DELAY_MS)
    await asyncio.sleep(delay_ms / 1000)

    notification = {
        "operation_id": event["operation_id"],
        "text": text,
        "sent_at_delay_ms": delay_ms,
    }
    notifications_store.setdefault(customer_id, []).append(notification)
    stats["notifications_sent"] += 1

    print(f"[NOTIFIER] {customer_id}: {text} (задержка {delay_ms} мс)", flush=True)


async def consume_events():
    connection = await aio_pika.connect_robust(AMQP_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=1)
    exchange = await channel.declare_exchange(EXCHANGE, ExchangeType.FANOUT, durable=True)
    queue = await channel.declare_queue(QUEUE, durable=True, arguments=QUEUE_ARGS)
    await queue.bind(exchange)

    print("Notifier started, waiting for events...", flush=True)

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
    return {"status": "ok", "service": "notifier"}


@app.get("/notifications")
async def get_notifications(customer_id: str = Query(...)):
    return notifications_store.get(customer_id, [])


@app.get("/stats")
async def get_stats():
    return stats