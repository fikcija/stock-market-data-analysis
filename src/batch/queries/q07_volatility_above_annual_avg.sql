
WITH volatility_with_avg AS (
    SELECT
        ticker,
        trade_date,
        year,
        rolling_volatility_30d,
        AVG(rolling_volatility_30d) OVER (
            PARTITION BY ticker, year
        ) AS annual_avg_volatility
    FROM daily_stock_metrics
    WHERE rolling_volatility_30d IS NOT NULL
)
SELECT
    ticker,
    trade_date,
    year,
    ROUND(rolling_volatility_30d, 6) AS rolling_volatility_30d,
    ROUND(annual_avg_volatility, 6) AS annual_avg_volatility,
    ROUND(rolling_volatility_30d - annual_avg_volatility, 6) AS volatility_above_avg
FROM volatility_with_avg
WHERE rolling_volatility_30d > annual_avg_volatility
ORDER BY ticker, trade_date;
