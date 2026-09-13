"""order-api — приём заказов и выдача списка для витрины."""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any

import asyncpg
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://orders:orders@postgres:5432/orders")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("order-api")

orders_accepted_total = Counter("orders_accepted_total", "Заказы, принятые API")
request_duration_seconds = Histogram(
    "order_request_duration_seconds",
    "Длительность обработки запроса",
    ["endpoint", "order_id"],
)


class Item(BaseModel):
    sku: str
    qty: int = Field(ge=1)


class Order(BaseModel):
    order_id: str
    customer_id: str
    amount: Decimal
    items: list[Item]


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    app.state.producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        acks=0,
        linger_ms=20,
    )
    await app.state.producer.start()
    log.info("order-api запущен, брокер %s, топик %s", KAFKA_BOOTSTRAP, ORDERS_TOPIC)
    yield
    await app.state.producer.stop()
    await app.state.pool.close()


app = FastAPI(title="FreshBox order-api", lifespan=lifespan)


@app.post("/orders", status_code=202)
async def create_order(order: Order) -> dict[str, str]:
    started = time.perf_counter()
    payload = json.dumps(
        {
            "order_id": order.order_id,
            "customer_id": order.customer_id,
            "amount": float(order.amount),
            "items": [item.model_dump() for item in order.items],
        }
    ).encode()

    # Кладём в очередь и сразу отвечаем клиенту — так укладываемся в 200 мс (Б1).
    asyncio.create_task(app.state.producer.send(ORDERS_TOPIC, payload))

    orders_accepted_total.inc()
    request_duration_seconds.labels(endpoint="POST /orders", order_id=order.order_id).observe(
        time.perf_counter() - started
    )
    log.info("заказ принят order_id=%s", order.order_id)
    return {"status": "accepted", "order_id": order.order_id}


@app.get("/orders")
async def list_orders(limit: int = 50) -> list[dict[str, Any]]:
    limit = max(1, min(limit, 500))
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT order_id, customer_id, amount, items, created_at "
            "FROM orders ORDER BY created_at DESC LIMIT $1",
            limit,
        )
    return [
        {
            "order_id": r["order_id"],
            "customer_id": r["customer_id"],
            "amount": float(r["amount"]),
            "items": json.loads(r["items"]),
            "created_at": r["created_at"].isoformat(),
        }
        for r in rows
    ]


@app.get("/orders/{order_id}")
async def get_order(order_id: str) -> dict[str, Any]:
    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT order_id, customer_id, amount, items, created_at "
            "FROM orders WHERE order_id = $1",
            order_id,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="заказ не найден")
    return {
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "amount": float(row["amount"]),
        "items": json.loads(row["items"]),
        "created_at": row["created_at"].isoformat(),
    }


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
