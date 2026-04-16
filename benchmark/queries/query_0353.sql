-- {"operators": "AGGREGATE,FILTER,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT * FROM (SELECT O_W_ID, O_D_ID, COUNT(*) as order_cnt FROM OORDER WHERE O_ALL_LOCAL = 1 GROUP BY O_W_ID, O_D_ID) WHERE order_cnt > 5;
