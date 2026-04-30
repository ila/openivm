-- {"operators": "LATERAL,AGGREGATE,FILTER,SUBQUERY", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "non_incr_reason": "op:LATERAL"}
SELECT d.D_W_ID, d.D_ID, d.D_NAME, lat.cust_count FROM DISTRICT d, LATERAL (SELECT COUNT(*) AS cust_count FROM CUSTOMER WHERE C_W_ID = d.D_W_ID AND C_D_ID = d.D_ID) lat;
