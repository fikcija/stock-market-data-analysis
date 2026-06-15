"""
Re-registers all Hive EXTERNAL tables after a metastore reset.
Safe to run multiple times — uses DROP IF EXISTS before each CREATE.
Run this after every docker-compose down or hive-metastore-postgresql recreation.
"""

import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RegisterHiveTables") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

spark.sql("CREATE DATABASE IF NOT EXISTS stock_analytics")
spark.sql("USE stock_analytics")

print("Registering exchange_dim...", flush=True)
spark.sql("DROP TABLE IF EXISTS exchange_dim")
spark.sql(f"""
    CREATE EXTERNAL TABLE exchange_dim (
        exchange_id     INT,
        exchange_name   STRING
    )
    STORED AS PARQUET
    LOCATION '{NAMENODE}/data/processed/exchange_dim/'
""")

print("Registering ticker_exchanges...", flush=True)
spark.sql("DROP TABLE IF EXISTS ticker_exchanges")
spark.sql(f"""
    CREATE EXTERNAL TABLE ticker_exchanges (
        ticker          STRING,
        exchange_id     INT
    )
    STORED AS PARQUET
    LOCATION '{NAMENODE}/data/processed/ticker_exchanges/'
""")

print("Registering stock_prices...", flush=True)
spark.sql("DROP TABLE IF EXISTS stock_prices")
spark.sql(f"""
    CREATE EXTERNAL TABLE stock_prices (
        ticker                  STRING,
        trade_date              DATE,
        quarter                 INT,
        day_of_week             INT,
        open_price              DOUBLE,
        high_price              DOUBLE,
        low_price               DOUBLE,
        close_price             DOUBLE,
        adjusted_close_price    DOUBLE,
        volume                  BIGINT,
        daily_range             DOUBLE,
        daily_change            DOUBLE,
        daily_change_pct        DOUBLE
    )
    PARTITIONED BY (year INT, month INT)
    STORED AS PARQUET
    LOCATION '{NAMENODE}/data/processed/stock_prices/'
""")
spark.sql("MSCK REPAIR TABLE stock_prices")

print("Registering daily_stock_metrics...", flush=True)
spark.sql("DROP TABLE IF EXISTS daily_stock_metrics")
spark.sql(f"""
    CREATE EXTERNAL TABLE daily_stock_metrics (
        ticker                  STRING,
        trade_date              DATE,
        open_price              DOUBLE,
        high_price              DOUBLE,
        low_price               DOUBLE,
        close_price             DOUBLE,
        adjusted_close_price    DOUBLE,
        volume                  BIGINT,
        daily_change_pct        DOUBLE,
        prev_close_price        DOUBLE,
        avg_volume_20d          BIGINT,
        avg_volume_7d           BIGINT,
        volume_ratio            DOUBLE,
        is_volume_spike         BOOLEAN,
        is_significant_drop     BOOLEAN,
        rolling_volatility_30d  DOUBLE,
        rolling_high_7d         DOUBLE,
        rolling_low_7d          DOUBLE
    )
    PARTITIONED BY (year INT)
    STORED AS PARQUET
    LOCATION '{NAMENODE}/data/processed/daily_stock_metrics/'
""")
spark.sql("MSCK REPAIR TABLE daily_stock_metrics")

print("Registering monthly_stock_performance...", flush=True)
spark.sql("DROP TABLE IF EXISTS monthly_stock_performance")
spark.sql(f"""
    CREATE EXTERNAL TABLE monthly_stock_performance (
        ticker                  STRING,
        first_trade_date        DATE,
        last_trade_date         DATE,
        trading_days            BIGINT,
        month_open_price        DOUBLE,
        month_close_price       DOUBLE,
        month_high_price        DOUBLE,
        month_low_price         DOUBLE,
        monthly_return_pct      DOUBLE,
        total_volume            BIGINT,
        avg_daily_volume        BIGINT,
        avg_intraday_range      DOUBLE,
        max_intraday_range      DOUBLE,
        avg_intraday_range_pct  DOUBLE
    )
    PARTITIONED BY (year INT, month INT)
    STORED AS PARQUET
    LOCATION '{NAMENODE}/data/processed/monthly_stock_performance/'
""")
spark.sql("MSCK REPAIR TABLE monthly_stock_performance")

print("Registering yearly_stock_performance...", flush=True)
spark.sql("DROP TABLE IF EXISTS yearly_stock_performance")
spark.sql(f"""
    CREATE EXTERNAL TABLE yearly_stock_performance (
        ticker                  STRING,
        first_trade_date        DATE,
        last_trade_date         DATE,
        trading_days            BIGINT,
        year_open_price         DOUBLE,
        year_close_price        DOUBLE,
        year_high_price         DOUBLE,
        year_low_price          DOUBLE,
        yearly_return_pct       DOUBLE,
        total_volume            BIGINT,
        avg_daily_volume        BIGINT,
        avg_daily_return_pct    DOUBLE,
        stddev_daily_return     DOUBLE,
        positive_days           BIGINT,
        negative_days           BIGINT
    )
    PARTITIONED BY (year INT)
    STORED AS PARQUET
    LOCATION '{NAMENODE}/data/processed/yearly_stock_performance/'
""")
spark.sql("MSCK REPAIR TABLE yearly_stock_performance")

print("Registering streaming_daily_quotes...", flush=True)
HDFS_DAILY_PATH = NAMENODE + "/data/streaming/daily_quotes/"
spark.sql("DROP TABLE IF EXISTS streaming_daily_quotes")
spark.sql(f"""
    CREATE EXTERNAL TABLE streaming_daily_quotes (
        symbol          STRING,
        exchange        ARRAY<STRING>,
        current_price   DOUBLE,
        change          DOUBLE,
        change_pct      DOUBLE,
        high_price      DOUBLE,
        low_price       DOUBLE,
        open_price      DOUBLE,
        prev_close      DOUBLE,
        api_timestamp   BIGINT
    )
    PARTITIONED BY (quote_date STRING)
    STORED AS PARQUET
    LOCATION '{HDFS_DAILY_PATH}'
""")
spark.sql("MSCK REPAIR TABLE streaming_daily_quotes")

print("Registering streaming_7d_aggregates...", flush=True)
HDFS_AGG_PATH = NAMENODE + "/data/streaming/7d_aggregates/"
spark.sql("DROP TABLE IF EXISTS streaming_7d_aggregates")
spark.sql(f"""
    CREATE EXTERNAL TABLE streaming_7d_aggregates (
        symbol          STRING,
        max_7d_high     DOUBLE,
        min_7d_low      DOUBLE,
        avg_prev_close  DOUBLE,
        days_count      INT
    )
    STORED AS PARQUET
    LOCATION '{HDFS_AGG_PATH}'
""")

print("All Hive tables registered successfully.", flush=True)
spark.stop()
