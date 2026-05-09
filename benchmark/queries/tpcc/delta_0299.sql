-- {"operators": "OUTER_JOIN,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "delta": true}
WITH w AS (SELECT W_ID FROM d_WAREHOUSE), d AS (SELECT D_W_ID FROM d_DISTRICT) SELECT w.W_ID FROM w LEFT JOIN d ON w.W_ID = d.D_W_ID;
