-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "delta": true}
SELECT o1.O_W_ID, o1.O_C_ID, COUNT(*) AS pairs FROM d_OORDER o1 JOIN d_OORDER o2 ON o1.O_W_ID = o2.O_W_ID AND o1.O_D_ID = o2.O_D_ID AND o1.O_C_ID = o2.O_C_ID AND o1.O_ID < o2.O_ID GROUP BY o1.O_W_ID, o1.O_C_ID;
