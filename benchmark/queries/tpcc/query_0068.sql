-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_D_ID, COUNT(*) as order_cnt, SUM(O_OL_CNT) as lines FROM OORDER GROUP BY O_W_ID, O_D_ID;
