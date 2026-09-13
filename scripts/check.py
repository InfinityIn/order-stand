#!/usr/bin/env python3
"""Сверка сохранности заказов для стенда order-stand.

Берёт журнал генератора нагрузки и сравнивает его с базой данных:
что API подтвердил клиенту — то обязано лежать в базе ровно один раз.

Скрипт менять нельзя — на приёмке используется копия из основной ветки.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg

DEFAULT_DSN = "postgresql://orders:orders@localhost:5432/orders"


def read_journal(path: Path) -> tuple[int, list[str]]:
    sent = 0
    accepted: list[str] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            sent += 1
            if 200 <= int(record.get("status", 0)) < 300:
                accepted.append(record["order_id"])
    return sent, accepted


def main() -> int:
    parser = argparse.ArgumentParser(description="сверка журнала с базой")
    parser.add_argument("--journal", default="data/sent.jsonl", help="файл журнала")
    parser.add_argument(
        "--dsn",
        default=os.getenv("DATABASE_URL", DEFAULT_DSN),
        help="подключение к Postgres",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=0,
        help="показать до N идентификаторов потерянных заказов",
    )
    args = parser.parse_args()

    journal_path = Path(args.journal)
    if not journal_path.exists():
        print(f"журнал не найден: {journal_path}", file=sys.stderr)
        return 2

    sent, accepted = read_journal(journal_path)
    if not accepted:
        print("в журнале нет ни одного заказа, принятого API", file=sys.stderr)
        return 2

    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT order_id, COUNT(*) FROM orders WHERE order_id = ANY(%s) GROUP BY order_id",
            (accepted,),
        )
        counts = dict(cur.fetchall())

    unique_in_db = len(counts)
    rows_in_db = sum(counts.values())
    lost = len(accepted) - unique_in_db
    duplicated = rows_in_db - unique_in_db

    print(f"отправлено запросов   {sent:>8}")
    print(f"принято API (2xx)     {len(accepted):>8}")
    print(f"найдено в базе        {rows_in_db:>8}")
    print(f"уникальных в базе     {unique_in_db:>8}")
    print("-" * 30)
    print(f"потеряно              {lost:>8}")
    print(f"дублировано           {duplicated:>8}")

    if args.show and lost:
        missing = [order_id for order_id in accepted if order_id not in counts]
        print("\nпотерянные заказы:")
        for order_id in missing[: args.show]:
            print(f"  {order_id}")

    ok = lost == 0 and duplicated == 0
    print(f"ИТОГ: {'СОШЛОСЬ' if ok else 'ПРОВАЛ'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
