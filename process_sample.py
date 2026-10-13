from ingestion import raw_storage
from pyspark.sql import SparkSession,functions as F
import os
import sys
def main():
    candles_data = raw_storage.read_data()
    if candles_data in (None,[]):
        print("No candle data was loaded.")
        return
    # reading candles data from json file
    candle_count = len(candles_data)
    fields_per_candle = len(candles_data[0])
    print(f"Loaded candles: {candle_count}")
    print(f"Fields in first candle: {fields_per_candle}")

    # initilizing spark 
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder
        .appName("MarketFlow")
        .master("local[2]")
        .config("spark.python.worker.faulthandler.enabled","True")
        .getOrCreate()
    )

    # constructing schema based on the api 
    schema = """
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

    # creating dataframe for candles
    df = spark.createDataFrame(candles_data,schema)

    # printing the dataframe and schema
    df.show(100,truncate=False)
    df.printSchema()
    print(f"DataFrame rows: {df.count()}") 

    # converting dataframes columns from text to decimal(28,8)

    typed_df = df.withColumns({
        "open": F.col("open").cast("decimal(28,8)"),
        "high": F.col("high").cast("decimal(28,8)"),
        "low": F.col("low").cast("decimal(28,8)"),
        "close": F.col("close").cast("decimal(28,8)"),
        "volume_btc": F.col("volume_btc").cast("decimal(28,8)"),
        "volume_usdt": F.col("volume_usdt").cast("decimal(28,8)"),
        "taker_buy_volume_btc": F.col("taker_buy_volume_btc").cast("decimal(28,8)"),
        "taker_buy_volume_usdt": F.col("taker_buy_volume_usdt").cast("decimal(28,8)")
    })

    # print the new data frame
    typed_df.show(truncate=False)
    typed_df.printSchema()
    print(f"Typed DataFrame rows: {typed_df.count()}")

    # check for bad prices relationships : ex: having a high lower than a low
    invalid_df = typed_df.filter("""
        high < low 
        OR open NOT BETWEEN low AND high
        OR close NOT BETWEEN low AND high
        """)

    print(f"Candles with bad prices relationships : {invalid_df.count()}")
   

    # check for missing values in candles (aka NULL columns)
    missing_values_df = typed_df.filter("""
        open IS NULL
        OR high IS NULL
        OR low IS NULL
        OR close IS NULL
        OR volume_btc IS NULL
        """)

    print(f"Candles with missing OHLCV values : {missing_values_df.count()}")

    spark.stop()

if __name__ == "__main__":
    main()