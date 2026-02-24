-- PostgreSQL schema for Spark Structured Streaming output
-- Database: streaming

-- SQ1: Intraday price volatility per symbol (10-min tumbling window)
CREATE TABLE IF NOT EXISTS sq1_price_volatility (
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    symbol              VARCHAR(20),
    avg_price           DOUBLE PRECISION,
    stddev_price        DOUBLE PRECISION,
    volatility_coeff    DOUBLE PRECISION,
    trade_count         BIGINT
);

-- SQ2: Market sentiment per exchange (1-min tumbling window)
CREATE TABLE IF NOT EXISTS sq2_market_sentiment (
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    exchange            VARCHAR(50),
    rising_count        BIGINT,
    falling_count       BIGINT,
    sentiment_ratio     DOUBLE PRECISION
);

-- SQ3: Trade frequency vs baseline (10-min tumbling window)
CREATE TABLE IF NOT EXISTS sq3_trade_frequency (
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    symbol              VARCHAR(20),
    trade_count         BIGINT,
    is_high_frequency   BOOLEAN
);

-- SQ4: Current price position vs 7-day high/low (5-min tumbling window)
CREATE TABLE IF NOT EXISTS sq4_price_vs_7d (
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    symbol              VARCHAR(20),
    current_price       DOUBLE PRECISION,
    max_7d_high         DOUBLE PRECISION,
    min_7d_low          DOUBLE PRECISION,
    pct_from_high       DOUBLE PRECISION,
    pct_from_low        DOUBLE PRECISION,
    position            VARCHAR(20)
);

-- SQ5: Current price deviation from previous close (1-min tumbling window)
CREATE TABLE IF NOT EXISTS sq5_vs_prev_close (
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    symbol              VARCHAR(20),
    current_price       DOUBLE PRECISION,
    prev_close          DOUBLE PRECISION,
    deviation_pct       DOUBLE PRECISION,
    direction           VARCHAR(10)
);