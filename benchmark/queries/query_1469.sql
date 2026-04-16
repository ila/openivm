-- {"operators": "FILTER,TABLE_FUNCTION,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT n, (SELECT W_NAME FROM WAREHOUSE WHERE W_ID = n) AS w_name FROM range(1, 11) t(n);
