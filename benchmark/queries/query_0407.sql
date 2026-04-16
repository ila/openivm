-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_D_ID, O_ALL_LOCAL, COUNT(*), SUM(O_OL_CNT) FROM OORDER GROUP BY O_W_ID, O_D_ID, O_ALL_LOCAL;
