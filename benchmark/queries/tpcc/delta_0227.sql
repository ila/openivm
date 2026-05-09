-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "delta": true}
SELECT d.D_W_ID, d.D_ID, COUNT(c.C_ID) AS active_custs FROM d_DISTRICT d LEFT JOIN d_CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID AND c.C_PAYMENT_CNT > 0 GROUP BY d.D_W_ID, d.D_ID;
