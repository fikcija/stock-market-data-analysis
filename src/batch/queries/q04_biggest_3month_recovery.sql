
WITH monthly_ordered AS (
    SELECT
        ticker,
        year,
        month,
        monthly_return_pct,
        LAG(monthly_return_pct) OVER (
            PARTITION BY ticker
            ORDER BY year, month
        ) AS prev_month_return_pct,
        SUM(monthly_return_pct) OVER (
            PARTITION BY ticker
            ORDER BY year, month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS rolling_3month_return
    FROM monthly_stock_performance
    WHERE monthly_return_pct IS NOT NULL
),
recoveries AS (
    SELECT
        ticker,
        year,
        month,
        monthly_return_pct,
        prev_month_return_pct,
        rolling_3month_return
    FROM monthly_ordered

    WHERE prev_month_return_pct < 0
      AND rolling_3month_return > 0
)
SELECT
    ticker,
    year,
    month,
    ROUND(prev_month_return_pct, 4) AS drop_month_return_pct,
    ROUND(rolling_3month_return, 4) AS rolling_3month_recovery_pct
FROM recoveries
ORDER BY rolling_3month_recovery_pct DESC
LIMIT 100;
