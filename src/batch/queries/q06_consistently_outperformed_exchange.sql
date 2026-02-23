USE stock_analytics;

WITH exchange_yearly_avg AS (
    SELECT
        te.exchange_id,
        ed.exchange_name,
        ysp.ticker,
        ysp.year,
        ysp.yearly_return_pct,
        AVG(ysp.yearly_return_pct) OVER (
            PARTITION BY te.exchange_id, ysp.year
        ) AS exchange_avg_return_pct
    FROM yearly_stock_performance ysp
    JOIN ticker_exchanges te ON ysp.ticker = te.ticker
    JOIN exchange_dim ed ON te.exchange_id = ed.exchange_id
    WHERE ysp.yearly_return_pct IS NOT NULL
),
outperformance AS (
    SELECT
        ticker,
        exchange_id,
        exchange_name,
        year,
        yearly_return_pct,
        exchange_avg_return_pct,
        CASE WHEN yearly_return_pct > exchange_avg_return_pct THEN 1 ELSE 0 END AS outperformed
    FROM exchange_yearly_avg
),
streak_count AS (
    SELECT
        ticker,
        exchange_id,
        exchange_name,
        SUM(outperformed) AS years_outperformed,
        COUNT(*) AS total_years
    FROM outperformance
    GROUP BY ticker, exchange_id, exchange_name
)
SELECT
    ticker,
    exchange_id,
    exchange_name,
    years_outperformed,
    total_years,
    ROUND(years_outperformed * 100.0 / total_years, 2) AS outperformance_rate_pct
FROM streak_count
WHERE years_outperformed >= 5
ORDER BY years_outperformed DESC, ticker;
