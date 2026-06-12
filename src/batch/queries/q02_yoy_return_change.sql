
SELECT
    ticker,
    year,
    yearly_return_pct,
    LAG(yearly_return_pct) OVER (
        PARTITION BY ticker
        ORDER BY year
    ) AS prev_year_return_pct,
    ROUND(
        yearly_return_pct - LAG(yearly_return_pct) OVER (
            PARTITION BY ticker
            ORDER BY year
        ),
        4
    ) AS yoy_return_change
FROM yearly_stock_performance
WHERE yearly_return_pct IS NOT NULL
ORDER BY ticker, year;
