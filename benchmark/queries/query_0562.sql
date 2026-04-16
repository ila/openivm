-- {"operators": "AGGREGATE,FILTER,TABLE_FUNCTION,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT C_W_ID, COUNT(*) AS cnt FROM CUSTOMER WHERE C_W_ID IN (SELECT * FROM generate_series(1, 5)) GROUP BY C_W_ID;
