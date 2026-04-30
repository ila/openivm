-- {"operators": "CTE,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
WITH wh AS (SELECT W_ID FROM WAREHOUSE) SELECT * FROM wh;
