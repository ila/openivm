-- {"operators": "FILTER,TABLE_FUNCTION,CTE,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
WITH numbers AS (SELECT generate_series AS n FROM generate_series(1, 20)) SELECT n, (SELECT W_NAME FROM WAREHOUSE WHERE W_ID = n) AS wh_name FROM numbers;
