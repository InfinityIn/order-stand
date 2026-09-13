"""order-worker — читает топик заказов и сохраняет заказы в Postgres."""

import asyncio
import json
import logging
import os

import asyncpg
from aiokafka import AIOKafkaConsumer
from prometheus_client import Counter, start_http_server

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "order-worker")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://orders:orders@postgres:5432/orders")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("order-worker")

orders_processed_total = Counter("orders_processed_total", "Обработанные заказы")


async def save_order(pool: asyncpg.Pool, order: dict) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO orders (order_id, customer_id, amount, items) VALUES ($1, $2, $3, $4)",
            order["order_id"],
            order["customer_id"],
            order["amount"],
            json.dumps(order["items"]),
        )


async def main() -> None:
    start_http_server(METRICS_PORT)

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    consumer = AIOKafkaConsumer(
        ORDERS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=1000,
    )
    await consumer.start()
    log.info("order-worker запущен, брокер %s, топик %s", KAFKA_BOOTSTRAP, ORDERS_TOPIC)

    async for message in consumer:
        # Заказы терять нельзя (Б2), поэтому повторяем обработку, пока она не удастся.
        while True:
            try:
                order = json.loads(message.value)
                orders_processed_total.inc()
                await save_order(pool, order)
                log.info("заказ сохранён order_id=%s", order["order_id"])
                break
            except Exception as exc:  # noqa: BLE001
                log.error("обработка не удалась, повтор: %s", exc)
                await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
