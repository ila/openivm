-- {"operators": "FILTER,DISTINCT,TABLE_FUNCTION,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT n FROM generate_series(1, 10) t(n) WHERE n IN (SELECT DISTINCT C_W_ID FROM CUSTOMER);
