-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT DATE_TRUNC('hour', O_ENTRY_D) AS hr, COUNT(*) AS orders, SUM(O_OL_CNT) AS total_lines FROM OORDER WHERE O_ENTRY_D IS NOT NULL GROUP BY DATE_TRUNC('hour', O_ENTRY_D);
