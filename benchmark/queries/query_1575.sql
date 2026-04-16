-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT DATE_TRUNC('hour', O_ENTRY_D) AS hour_bucket, COUNT(*) AS orders FROM OORDER WHERE O_ENTRY_D IS NOT NULL GROUP BY DATE_TRUNC('hour', O_ENTRY_D);
