-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, COUNT(*) AS cnt, SUM(O_OL_CNT) AS total FROM OORDER WHERE O_OL_CNT > 5 GROUP BY O_W_ID;
