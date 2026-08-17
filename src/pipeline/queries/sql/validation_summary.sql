-- Count of validation outcomes per (dataset, source, stage), most recent run first.
SELECT
    run_id,
    dataset,
    source,
    stage,
    COUNT(*)                                                 AS checks,
    COUNT(*) FILTER (WHERE passed)                           AS passed,
    COUNT(*) FILTER (WHERE NOT passed AND severity = 'ERROR')   AS errors,
    COUNT(*) FILTER (WHERE NOT passed AND severity = 'WARNING') AS warnings
FROM validation_results
GROUP BY run_id, dataset, source, stage
ORDER BY run_id DESC, dataset, source;
