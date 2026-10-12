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
    schema = "id INT"
    e_rdd = spark.sparkContext.emptyRDD()
    df = spark.createDataFrame(e_rdd, schema)

    added_row = [[0],[1],[2],[3],[4]]
    
    added_df = spark.createDataFrame(added_row,schema)

    df = df.union(added_df)

    df.show()
    df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()