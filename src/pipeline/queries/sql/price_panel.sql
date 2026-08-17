-- Daily close-price panel for the default ETF basket (SPY / TLT / GLD), pivoted so each
-- instrument is a column. Ready to feed a returns calculation.
SELECT
    date,
    MAX(CASE WHEN ticker = 'SPY' THEN close END) AS spy,
    MAX(CASE WHEN ticker = 'TLT' THEN close END) AS tlt,
    MAX(CASE WHEN ticker = 'GLD' THEN close END) AS gld
FROM market_prices
WHERE ticker IN ('SPY', 'TLT', 'GLD')
GROUP BY date
ORDER BY date;
