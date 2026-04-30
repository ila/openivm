-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_C_ID, COUNT(DISTINCT O_OL_CNT) AS unique_vals FROM OORDER GROUP BY O_C_ID;
