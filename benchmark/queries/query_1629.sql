-- {"operators": "INNER_JOIN,LATERAL,AGGREGATE,FILTER,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_ID, w.W_NAME, lat.n, lat.total FROM WAREHOUSE w JOIN LATERAL (SELECT COUNT(*) AS n, SUM(c.C_BALANCE) AS total FROM CUSTOMER c WHERE c.C_W_ID = w.W_ID) lat ON TRUE;
