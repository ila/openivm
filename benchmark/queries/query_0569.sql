-- {"operators": "LATERAL,AGGREGATE,FILTER,SUBQUERY", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER", "non_incr_reason": "op:LATERAL"}
SELECT w.W_ID, w.W_YTD, lat.total_orders FROM WAREHOUSE w, LATERAL (SELECT COUNT(*) AS total_orders FROM OORDER WHERE O_W_ID = w.W_ID) lat;
