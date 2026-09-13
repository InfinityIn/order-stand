#!/usr/bin/env python3
"""Генератор нагрузки для стенда order-stand.

Шлёт заказы в API с заданной скоростью и ведёт журнал отправленного: чекер
сверяет по нему принятое API с тем, что доехало до базы.

Скрипт менять нельзя — на приёмке используется копия из основной ветки.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
import uuid
from pathlib import Path

import httpx

SKUS = [
    ("MILK-1L", 129.00),
    ("BREAD-500", 79.50),
    ("EGGS-10", 189.00),
    ("APPLE-1KG", 219.00),
    ("COFFEE-250", 749.00),
    ("CHEESE-200", 399.00),
]


def make_order() -> dict:
    items = []
    total = 0.0
    for sku, price in random.sample(SKUS, k=random.randint(1, 3)):
        qty = random.randint(1, 3)
        items.append({"sku": sku, "qty": qty})
        total += price * qty
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"cust-{random.randint(10000, 99999)}",
        "amount": round(total, 2),
        "items": items,
    }


async def send_one(
    client: httpx.AsyncClient,
    api: str,
    journal,
    semaphore: asyncio.Semaphore,
    stats: dict,
) -> None:
    order = make_order()
    async with semaphore:
        started = time.perf_counter()
        sent_at = time.time()
        status = 0
        error = ""
        try:
            response = await client.post(f"{api}/orders", json=order)
            status = response.status_code
        except Exception as exc:  # noqa: BLE001
            error = f"{type(exc).__name__}: {exc}"
        elapsed_ms = (time.perf_counter() - started) * 1000

    journal.write(
        json.dumps(
            {
                "order_id": order["order_id"],
                "sent_at": sent_at,
                "status": status,
                "elapsed_ms": round(elapsed_ms, 2),
                "error": error,
            }
        )
        + "\n"
    )
    stats["done"] += 1
    stats["latencies"].append(elapsed_ms)
    if 200 <= status < 300:
        stats["accepted"] += 1
    else:
        stats["failed"] += 1
    if stats["done"] % 200 == 0:
        journal.flush()


async def run(args: argparse.Namespace) -> None:
    total = int(args.rate * args.duration)
    journal_path = Path(args.journal)
    journal_path.parent.mkdir(parents=True, exist_ok=True)

    stats = {"done": 0, "accepted": 0, "failed": 0, "latencies": []}
    semaphore = asyncio.Semaphore(args.concurrency)
    tasks: list[asyncio.Task] = []

    print(
        f"нагрузка: {args.rate} заказов/с × {args.duration} с = {total} запросов "
        f"на {args.api}",
        flush=True,
    )

    with journal_path.open("w", encoding="utf-8") as journal:
        limits = httpx.Limits(max_connections=args.concurrency * 2)
        async with httpx.AsyncClient(timeout=args.timeout, limits=limits) as client:
            loop = asyncio.get_running_loop()
            begin = loop.time()
            for i in range(total):
                target = begin + i / args.rate
                delay = target - loop.time()
                if delay > 0:
                    await asyncio.sleep(delay)
                tasks.append(
                    asyncio.create_task(send_one(client, args.api, journal, semaphore, stats))
                )
            await asyncio.gather(*tasks)
        journal.flush()

    latencies = sorted(stats["latencies"])

    def pct(p: float) -> float:
        if not latencies:
            return 0.0
        idx = min(len(latencies) - 1, int(len(latencies) * p))
        return latencies[idx]

    print()
    print(f"отправлено запросов   {total:>8}")
    print(f"принято API (2xx)     {stats['accepted']:>8}")
    print(f"ошибки и таймауты     {stats['failed']:>8}")
    print(f"p50 задержки, мс      {pct(0.50):>8.1f}")
    print(f"p95 задержки, мс      {pct(0.95):>8.1f}")
    print(f"журнал: {journal_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="генератор заказов")
    parser.add_argument("--api", default="http://localhost:8000", help="адрес API")
    parser.add_argument("--rate", type=float, default=50, help="заказов в секунду")
    parser.add_argument("--duration", type=float, default=60, help="длительность, с")
    parser.add_argument("--journal", default="data/sent.jsonl", help="файл журнала")
    parser.add_argument("--concurrency", type=int, default=100, help="одновременных запросов")
    parser.add_argument("--timeout", type=float, default=10, help="таймаут запроса, с")
    args = parser.parse_args()

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nпрервано, журнал сохранён")


if __name__ == "__main__":
    main()
