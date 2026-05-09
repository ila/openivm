-- {"operators": "ORDER,WINDOW,CTE,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "delta": true}
WITH ordered AS (SELECT O_W_ID, O_D_ID, O_ID, O_C_ID, O_ENTRY_D FROM d_OORDER) SELECT O_W_ID, O_D_ID, O_ID, O_C_ID, LAG(O_C_ID, 1) OVER (PARTITION BY O_W_ID, O_D_ID ORDER BY O_ID) AS prev_customer FROM ordered;
