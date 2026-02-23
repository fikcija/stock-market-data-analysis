USE stock_analytics;

WITH spike_days AS (
    SELECT
        ticker,
        trade_date,
        year,
        volume,
        avg_volume_20d,
        volume_ratio,
        daily_change_pct AS spike_day_change_pct,
        LEAD(daily_change_pct) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
        ) AS next_day_change_pct
    FROM daily_stock_metrics
    WHERE is_volume_spike = TRUE
)
SELECT
    ticker,
    trade_date,
    year,
    volume,
    avg_volume_20d,
    ROUND(volume_ratio, 4) AS volume_ratio,
    ROUND(spike_day_change_pct, 6) AS spike_day_change_pct,
    ROUND(next_day_change_pct, 6) AS next_day_change_pct,
    CASE
        WHEN next_day_change_pct > 0 THEN 'up'
        WHEN next_day_change_pct < 0 THEN 'down'
        ELSE 'flat'
    END AS next_day_direction
FROM spike_days
WHERE next_day_change_pct IS NOT NULL
ORDER BY ticker, trade_date;
