-- {"operators": "AGGREGATE,UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,OORDER"}
SELECT O_W_ID, SUM(O_OL_CNT) FROM OORDER GROUP BY O_W_ID UNION SELECT S_W_ID, SUM(S_QUANTITY) FROM STOCK GROUP BY S_W_ID;
