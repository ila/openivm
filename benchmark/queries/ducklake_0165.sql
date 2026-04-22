-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "non_incr_reason": "op:SUBQUERY_FILTER", "ducklake": true}
SELECT d.D_W_ID, d.D_ID, d.D_NAME, (SELECT COUNT(*) FROM dl.CUSTOMER c WHERE c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID) AS custs FROM dl.DISTRICT d;
