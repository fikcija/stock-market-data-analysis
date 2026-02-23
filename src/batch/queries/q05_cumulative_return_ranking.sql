USE stock_analytics;

WITH cumulative AS (
    SELECT
        ticker,
        year,
        month,
        monthly_return_pct,
        SUM(monthly_return_pct) OVER (
            PARTITION BY ticker, year
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_return_pct
    FROM monthly_stock_performance
    WHERE monthly_return_pct IS NOT NULL
)
SELECT
    ticker,
    year,
    month,
    ROUND(monthly_return_pct, 4) AS monthly_return_pct,
    ROUND(cumulative_return_pct, 4) AS cumulative_return_pct,
    RANK() OVER (
        PARTITION BY year, month
        ORDER BY cumulative_return_pct DESC
    ) AS cumulative_rank
FROM cumulative
ORDER BY year, month, cumulative_rank;
