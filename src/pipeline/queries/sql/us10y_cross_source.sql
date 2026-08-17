-- Latest available US 10-year Treasury yield, one row per source, for cross-checking.
-- The three public sources (Yahoo ^TNX, FRED DGS10, AKShare) should agree to within a
-- few basis points; a large spread is the signal the cross-source consistency check
-- raises.
SELECT
    source,
    date,
    value,
    unit
FROM macro_data
WHERE indicator = 'US_TREASURY_10Y'
ORDER BY date DESC, source;
