-- {"operators": "ORDER,UNION,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER", "delta": true}
SELECT O_W_ID AS w, O_ID AS id, O_C_ID AS cust, ROW_NUMBER() OVER (PARTITION BY O_W_ID ORDER BY O_ID) AS rn FROM d_OORDER UNION ALL SELECT NO_W_ID, NO_O_ID, -1, 0 FROM d_NEW_ORDER;
