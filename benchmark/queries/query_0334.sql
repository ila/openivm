-- {"operators": "OUTER_JOIN,AGGREGATE,FILTER,DISTINCT,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
WITH w_count AS (SELECT W_ID, COUNT(DISTINCT D_ID) as dist_cnt FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY W_ID) SELECT * FROM w_count WHERE dist_cnt > 0;
