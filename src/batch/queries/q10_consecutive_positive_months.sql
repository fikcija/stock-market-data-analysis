USE stock_analytics;

WITH monthly_flags AS (
    SELECT
        ticker,
        year,
        month,
        monthly_return_pct,
        CASE WHEN monthly_return_pct > 0 THEN 1 ELSE 0 END AS is_positive
    FROM monthly_stock_performance
    WHERE monthly_return_pct IS NOT NULL
),

lagged AS (
    SELECT
        ticker,
        year,
        month,
        monthly_return_pct,
        is_positive,
        LAG(is_positive, 1, is_positive) OVER (
            PARTITION BY ticker ORDER BY year, month
        ) AS prev_is_positive
    FROM monthly_flags
),

streak_groups AS (
    SELECT
        ticker,
        year,
        month,
        monthly_return_pct,
        is_positive,
        SUM(CASE WHEN is_positive = prev_is_positive THEN 0 ELSE 1 END)
            OVER (
                PARTITION BY ticker ORDER BY year, month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS streak_id
    FROM lagged
),

streak_lengths AS (
    SELECT
        ticker,
        streak_id,
        MIN(year) AS streak_start_year,
        MIN(month) AS streak_start_month,
        MAX(year) AS streak_end_year,
        MAX(month) AS streak_end_month,
        COUNT(*) AS consecutive_months,
        MIN(is_positive) AS all_positive  -- 1 if entire group is positive
    FROM streak_groups
    GROUP BY ticker, streak_id
)
SELECT
    ticker,
    streak_start_year,
    streak_start_month,
    streak_end_year,
    streak_end_month,
    consecutive_months
FROM streak_lengths
WHERE all_positive = 1
  AND consecutive_months >= 6
ORDER BY consecutive_months DESC, ticker;
