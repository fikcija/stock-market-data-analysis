import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("LoadRawLayer").setMaster("spark://spark-master:7077")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Low", StringType(), True),
    StructField("Open", StringType(), True),
    StructField("Volume", StringType(), True),
    StructField("High", StringType(), True),
    StructField("Close", StringType(), True),
    StructField("Adjusted Close", StringType(), True),
])

exchanges = ["nasdaq", "nyse", "sp500", "forbes2000"]
output_path = HDFS_NAMENODE + "/data/raw/stock_prices"

for exchange in exchanges:
    csv_path = f"/data/stock_market_data/{exchange}/csv/"

    if not os.path.exists(csv_path):
        print(f"Skipping {exchange}: path not found")
        continue

    df = spark.read \
        .option("header", "true") \
        .option("mergeSchema", "false") \
        .schema(schema) \
        .csv(f"{csv_path}*.csv") \
        .withColumn("_input_file", F.input_file_name()) \
        .withColumn("ticker", F.regexp_extract(F.col("_input_file"), r"([^/]+)\.csv$", 1)) \
        .select(
            F.col("ticker"),
            F.to_date(F.col("Date"), "dd-MM-yyyy").alias("date"),
            F.col("Open").cast(DoubleType()).alias("open"),
            F.col("High").cast(DoubleType()).alias("high"),
            F.col("Low").cast(DoubleType()).alias("low"),
            F.col("Close").cast(DoubleType()).alias("close"),
            F.col("Adjusted Close").cast(DoubleType()).alias("adjusted_close"),
            F.col("Volume").cast(LongType()).alias("volume"),
            F.lit(exchange).alias("source_exchange"),
            F.current_timestamp().alias("ingestion_timestamp")
        ) \
        .filter(F.col("date") >= "1993-01-01")

    df.write \
        .mode("overwrite") \
        .parquet(f"{output_path}/exchange={exchange}/")

spark.stop()