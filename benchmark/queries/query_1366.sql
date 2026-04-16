-- {"operators": "INNER_JOIN,LATERAL,AGGREGATE,FILTER,SUBQUERY", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "non_incr_reason": "op:LATERAL"}
SELECT w.W_ID, w.W_NAME, lat.num FROM WAREHOUSE w JOIN LATERAL (SELECT COUNT(*) AS num FROM DISTRICT WHERE D_W_ID = w.W_ID) lat ON TRUE;
