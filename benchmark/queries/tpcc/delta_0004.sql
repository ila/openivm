-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER", "delta": true}
SELECT o.O_W_ID, o.O_D_ID, o.O_C_ID, COUNT(*) AS order_count FROM d_CUSTOMER c JOIN d_OORDER o ON c.C_W_ID = o.O_W_ID AND c.C_D_ID = o.O_D_ID AND c.C_ID = o.O_C_ID GROUP BY o.O_W_ID, o.O_D_ID, o.O_C_ID;
