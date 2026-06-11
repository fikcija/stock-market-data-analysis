
WITH ranked AS (
    SELECT
        te.exchange_id,
        ed.exchange_name,
        ysp.ticker,
        ysp.year,
        ysp.yearly_return_pct,
        RANK() OVER (
            PARTITION BY te.exchange_id, ysp.year
            ORDER BY ysp.yearly_return_pct DESC
        ) AS return_rank
    FROM yearly_stock_performance ysp
    JOIN ticker_exchanges te ON ysp.ticker = te.ticker
    JOIN exchange_dim ed ON te.exchange_id = ed.exchange_id
    WHERE ysp.yearly_return_pct IS NOT NULL
)
SELECT
    exchange_id,
    exchange_name,
    year,
    ticker,
    yearly_return_pct,
    return_rank
FROM ranked
WHERE return_rank <= 3
ORDER BY exchange_id, year, return_rank;
