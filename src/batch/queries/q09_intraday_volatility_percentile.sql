
SELECT
    msp.ticker,
    te.exchange_id,
    ed.exchange_name,
    msp.year,
    msp.month,
    ROUND(msp.avg_intraday_range_pct, 4) AS avg_intraday_range_pct,
    ROUND(
        PERCENT_RANK() OVER (
            PARTITION BY te.exchange_id, msp.year, msp.month
            ORDER BY msp.avg_intraday_range_pct
        ),
        4
    ) AS volatility_percentile
FROM monthly_stock_performance msp
JOIN ticker_exchanges te ON msp.ticker = te.ticker
JOIN exchange_dim ed ON te.exchange_id = ed.exchange_id
WHERE msp.avg_intraday_range_pct IS NOT NULL
ORDER BY msp.year, msp.month, te.exchange_id, volatility_percentile DESC;
