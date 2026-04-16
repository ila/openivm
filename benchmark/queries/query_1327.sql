-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT w.W_ID, w.W_NAME FROM WAREHOUSE w WHERE (SELECT COUNT(*) FROM DISTRICT d WHERE d.D_W_ID = w.W_ID) > 5;
