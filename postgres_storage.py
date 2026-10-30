import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


load_dotenv(Path(__file__).resolve().parent / ".env")


COLUMNS = [
    "symbol",
    "interval",
    "open_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume_btc",
    "volume_usdt",
    "trade_count",
    "taker_buy_volume_btc",
    "taker_buy_volume_usdt",
    "price_change",
    "price_change_pct",
]


UPSERT_SQL = """
    INSERT INTO candles (
        symbol, interval, open_time, close_time,
        open, high, low, close,
        volume_btc, volume_usdt, trade_count,
        taker_buy_volume_btc, taker_buy_volume_usdt,
        price_change, price_change_pct
    )
    VALUES (
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s,
        %s, %s,
        %s, %s
    )
    ON CONFLICT (symbol, interval, open_time)
    DO UPDATE SET
        close_time = EXCLUDED.close_time,
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume_btc = EXCLUDED.volume_btc,
        volume_usdt = EXCLUDED.volume_usdt,
        trade_count = EXCLUDED.trade_count,
        taker_buy_volume_btc = EXCLUDED.taker_buy_volume_btc,
        taker_buy_volume_usdt = EXCLUDED.taker_buy_volume_usdt,
        price_change = EXCLUDED.price_change,
        price_change_pct = EXCLUDED.price_change_pct,
        updated_at = CURRENT_TIMESTAMP
"""


def save_candles(df):
    saved_count = 0
    batch = []

    with psycopg.connect(
        host=os.environ["PGHOST"],
        port=os.environ["PGPORT"],
        dbname=os.environ["PGDATABASE"],
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        connect_timeout=10,
      ) as connection:
        with connection.cursor() as cursor:
            for row in df.select(*COLUMNS).toLocalIterator():
                batch.append(tuple(row))

                if len(batch) >= 1000:
                    cursor.executemany(UPSERT_SQL, batch)
                    saved_count += len(batch)
                    batch.clear()

            if batch:
                cursor.executemany(UPSERT_SQL, batch)
                saved_count += len(batch)

    return saved_count