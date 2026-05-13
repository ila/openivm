-- {"operators": "FILTER,TABLE_FUNCTION,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT n, (SELECT W_NAME FROM WAREHOUSE WHERE W_ID = n) AS w_name FROM range(1, 11) t(n);
