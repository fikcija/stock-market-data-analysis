
WITH monthly_with_avg AS (
    SELECT
        ticker,
        year,
        month,
        monthly_return_pct,
        AVG(monthly_return_pct) OVER (
            PARTITION BY ticker
        ) AS avg_monthly_return_pct,
        monthly_return_pct - AVG(monthly_return_pct) OVER (
            PARTITION BY ticker
        ) AS return_vs_avg
    FROM monthly_stock_performance
    WHERE monthly_return_pct IS NOT NULL
),
ranked AS (
    SELECT
        year,
        month,
        ticker,
        monthly_return_pct,
        avg_monthly_return_pct,
        return_vs_avg,
        RANK() OVER (
            PARTITION BY year, month
            ORDER BY return_vs_avg ASC
        ) AS drop_rank
    FROM monthly_with_avg
)
SELECT
    year,
    month,
    ticker,
    ROUND(monthly_return_pct, 4) AS monthly_return_pct,
    ROUND(avg_monthly_return_pct, 4) AS avg_monthly_return_pct,
    ROUND(return_vs_avg, 4) AS return_vs_avg
FROM ranked
WHERE drop_rank = 1
ORDER BY year, month;
