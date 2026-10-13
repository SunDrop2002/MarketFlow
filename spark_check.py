from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
import sys



def main():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (
    SparkSession.builder
    .appName("MarketFlowCheck")
    .master("local[2]")
    .config("spark.python.worker.faulthandler.enabled", "true")
    .getOrCreate()
    )
    schema = "high INT,low INT"
    e_rdd = spark.sparkContext.emptyRDD()
    df = spark.createDataFrame(e_rdd, schema)

    added_row = [[110,100],[90,100]]
    
    added_df = spark.createDataFrame(added_row,schema)

    df = df.union(added_df)

    df.show()
    df.printSchema()

    invalid_df = df.filter("high < low")

    print(f"Candles with high < low: {invalid_df.count()}")    
    spark.stop()


if __name__ == "__main__":
    main()