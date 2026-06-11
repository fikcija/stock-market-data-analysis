CREATE DATABASE IF NOT EXISTS stock_analytics;
USE stock_analytics;

DROP TABLE IF EXISTS exchange_dim;
CREATE TABLE exchange_dim (
    exchange_id     INT,
    exchange_name   STRING
);

DROP TABLE IF EXISTS ticker_exchanges;
CREATE EXTERNAL TABLE ticker_exchanges (
    ticker          STRING,
    exchange_id     INT
)
STORED AS PARQUET
LOCATION '${HDFS_NAMENODE}/data/processed/ticker_exchanges/';

DROP TABLE IF EXISTS stock_prices;
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
LOCATION '${HDFS_NAMENODE}/data/processed/stock_prices/';
MSCK REPAIR TABLE stock_prices;

DROP TABLE IF EXISTS daily_stock_metrics;
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
LOCATION '${HDFS_NAMENODE}/data/processed/daily_stock_metrics/';
MSCK REPAIR TABLE daily_stock_metrics;

DROP TABLE IF EXISTS monthly_stock_performance;
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
LOCATION '${HDFS_NAMENODE}/data/processed/monthly_stock_performance/';
MSCK REPAIR TABLE monthly_stock_performance;

DROP TABLE IF EXISTS yearly_stock_performance;
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
LOCATION '${HDFS_NAMENODE}/data/processed/yearly_stock_performance/';
MSCK REPAIR TABLE yearly_stock_performance;
