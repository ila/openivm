-- {"operators": "CTE,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "delta": true}
WITH wh AS (SELECT W_ID FROM d_WAREHOUSE) SELECT * FROM wh;
