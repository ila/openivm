-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_D_ID, O_ALL_LOCAL, COUNT(*) as cnt, AVG(O_OL_CNT) as avg_lines FROM OORDER GROUP BY O_W_ID, O_D_ID, O_ALL_LOCAL;
