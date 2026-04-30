-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_D_ID, COUNT(*) AS order_cnt, AVG(O_OL_CNT) AS avg_lines FROM OORDER GROUP BY O_W_ID, O_D_ID HAVING AVG(O_OL_CNT) > 5;
