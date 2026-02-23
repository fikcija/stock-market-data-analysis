-- Create database
CREATE DATABASE IF NOT EXISTS stock_analytics;
USE stock_analytics;

-- Table 1: exchange_dim (managed table, seeded with known exchanges)
CREATE TABLE IF NOT EXISTS exchange_dim (
    exchange_id INT,
    exchange_name STRING
);
INSERT OVERWRITE TABLE exchange_dim
SELECT 1 AS exchange_id, 'nasdaq' AS exchange_name
UNION ALL SELECT 2, 'nyse'
UNION ALL SELECT 3, 'sp500'
UNION ALL SELECT 4, 'forbes2000';

-- Table 2: ticker_exchanges (bridge table — ticker to exchange mapping)
CREATE EXTERNAL TABLE IF NOT EXISTS ticker_exchanges (
    ticker STRING,
    exchange_id INT
)
STORED AS PARQUET
LOCATION 'hdfs:///data/processed/ticker_exchanges/';

-- Table 3: stock_prices (base processed table)
CREATE EXTERNAL TABLE IF NOT EXISTS stock_prices (
    ticker STRING,
    trade_date DATE,
    quarter INT,
    day_of_week INT,
    open_price DECIMAL(18,6),
    high_price DECIMAL(18,6),
    low_price DECIMAL(18,6),
    close_price DECIMAL(18,6),
    adjusted_close_price DECIMAL(18,6),
    volume BIGINT,
    daily_range DECIMAL(18,6),
    daily_change DECIMAL(18,6),
    daily_change_pct DECIMAL(10,6)
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs:///data/processed/stock_prices/';

-- Table 4: daily_stock_metrics
CREATE EXTERNAL TABLE IF NOT EXISTS daily_stock_metrics (
    ticker STRING,
    trade_date DATE,
    open_price DECIMAL(18,6),
    high_price DECIMAL(18,6),
    low_price DECIMAL(18,6),
    close_price DECIMAL(18,6),
    adjusted_close_price DECIMAL(18,6),
    volume BIGINT,
    daily_change_pct DECIMAL(10,6),
    prev_close_price DECIMAL(18,6),
    avg_volume_20d BIGINT,
    avg_volume_7d BIGINT,
    volume_ratio DOUBLE,
    is_volume_spike BOOLEAN,
    is_significant_drop BOOLEAN,
    rolling_volatility_30d DOUBLE,
    rolling_high_7d DECIMAL(18,6),
    rolling_low_7d DECIMAL(18,6)
)
PARTITIONED BY (year INT)
STORED AS PARQUET
LOCATION 'hdfs:///data/processed/daily_stock_metrics/';

-- Table 5: monthly_stock_performance
CREATE EXTERNAL TABLE IF NOT EXISTS monthly_stock_performance (
    ticker STRING,
    first_trade_date DATE,
    last_trade_date DATE,
    trading_days BIGINT,
    month_open_price DECIMAL(18,6),
    month_close_price DECIMAL(18,6),
    month_high_price DECIMAL(18,6),
    month_low_price DECIMAL(18,6),
    monthly_return_pct DECIMAL(10,4),
    total_volume BIGINT,
    avg_daily_volume BIGINT,
    avg_intraday_range DECIMAL(18,6),
    max_intraday_range DECIMAL(18,6),
    avg_intraday_range_pct DECIMAL(10,4)
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 'hdfs:///data/processed/monthly_stock_performance/';

-- Table 6: yearly_stock_performance
CREATE EXTERNAL TABLE IF NOT EXISTS yearly_stock_performance (
    ticker STRING,
    first_trade_date DATE,
    last_trade_date DATE,
    trading_days BIGINT,
    year_open_price DECIMAL(18,6),
    year_close_price DECIMAL(18,6),
    year_high_price DECIMAL(18,6),
    year_low_price DECIMAL(18,6),
    yearly_return_pct DECIMAL(10,4),
    total_volume BIGINT,
    avg_daily_volume BIGINT,
    avg_daily_return_pct DECIMAL(10,6),
    stddev_daily_return DECIMAL(10,6),
    positive_days BIGINT,
    negative_days BIGINT
)
PARTITIONED BY (year INT)
STORED AS PARQUET
LOCATION 'hdfs:///data/processed/yearly_stock_performance/';

-- Repair partitions for all partitioned external tables
MSCK REPAIR TABLE stock_prices;
MSCK REPAIR TABLE daily_stock_metrics;
MSCK REPAIR TABLE monthly_stock_performance;
MSCK REPAIR TABLE yearly_stock_performance;
