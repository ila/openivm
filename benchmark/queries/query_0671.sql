-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, DATE_TRUNC('month', O_ENTRY_D) AS month, COUNT(*) AS orders, SUM(O_OL_CNT) AS total_lines FROM OORDER GROUP BY O_W_ID, DATE_TRUNC('month', O_ENTRY_D);
