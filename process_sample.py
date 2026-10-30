import logging
import os
import sys

from pyspark.sql import SparkSession, functions as F

from ingestion import raw_storage
from postgres_storage import save_candles


logger = logging.getLogger(__name__)

SCHEMA = """
    open_time LONG,
    open STRING,
    high STRING,
    low STRING,
    close STRING,
    volume_btc STRING,
    close_time LONG,
    volume_usdt STRING,
    trade_count LONG,
    taker_buy_volume_btc STRING,
    taker_buy_volume_usdt STRING,
    unused STRING
"""

DECIMAL_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume_btc",
    "volume_usdt",
    "taker_buy_volume_btc",
    "taker_buy_volume_usdt",
]

INVALID_CONDITION = """
    open IS NULL
    OR high IS NULL
    OR low IS NULL
    OR close IS NULL
    OR volume_btc IS NULL
    OR open_time IS NULL
    OR close_time IS NULL
    OR open <= 0
    OR high <= 0
    OR low <= 0
    OR close <= 0
    OR volume_btc < 0
    OR high < low
    OR open NOT BETWEEN low AND high
    OR close NOT BETWEEN low AND high
    OR close_time < open_time
"""


def create_spark_session():
    os.environ["PYSPARK_PYTHON"] = sys.executable

    return (
        SparkSession.builder
        .appName("MarketFlow")
        .master("local[2]")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def convert_types(df):
    return df.withColumns({
        column: F.col(column).cast("decimal(28,8)")
        for column in DECIMAL_COLUMNS
    })


def enrich_candles(df, symbol, interval):
    return df.withColumns({
        "open_timestamp": F.timestamp_millis(F.col("open_time")),
        "close_timestamp": F.timestamp_millis(F.col("close_time")),
        "price_change": F.expr("close - open"),
        "price_change_pct": F.expr("((close - open) / open) * 100"),
        "symbol": F.lit(symbol),
        "interval": F.lit(interval),
    })


def check_duplicates(spark, df):
    duplicates = spark.sql("""
        SELECT symbol, interval, open_time, COUNT(*) AS occurrences
        FROM {candles}
        GROUP BY symbol, interval, open_time
        HAVING COUNT(*) > 1
    """, candles=df)

    if duplicates.limit(1).count() > 0:
        raise ValueError(
            "Duplicate candle keys found. Database load stopped."
        )


def main():
    candles_data = raw_storage.read_data()

    if not candles_data:
        raise ValueError("No candle data was loaded. Processing stopped.")

    input_count = len(candles_data)
    logger.info("Loaded %s raw candles.", input_count)

    spark = create_spark_session()
    typed_df = None

    try:
        raw_df = spark.createDataFrame(candles_data, SCHEMA)
        typed_df = convert_types(raw_df).cache()

        rejected_count = typed_df.filter(INVALID_CONDITION).count()
        accepted_count = input_count - rejected_count

        logger.info(
            "Validation: input=%s, accepted=%s, rejected=%s",
            input_count,
            accepted_count,
            rejected_count,
        )

        if rejected_count:
            logger.warning(
                "%s invalid candles excluded from the database load.",
                rejected_count,
            )

        if accepted_count == 0:
            raise ValueError("No valid candles remain. Database load stopped.")

        accepted_df = typed_df.filter(f"NOT ({INVALID_CONDITION})")

        enriched_df = enrich_candles(
            accepted_df,
            symbol="BTCUSDT",
            interval="1m",
        )

        check_duplicates(spark, enriched_df)

        saved_count = save_candles(enriched_df)
        logger.info("Candles written to PostgreSQL: %s", saved_count)

    finally:
        if typed_df is not None:
            typed_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    main()