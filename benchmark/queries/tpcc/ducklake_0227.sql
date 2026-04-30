-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "ducklake": true}
SELECT d.D_W_ID, d.D_ID, COUNT(c.C_ID) AS active_custs FROM dl.DISTRICT d LEFT JOIN dl.CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID AND c.C_PAYMENT_CNT > 0 GROUP BY d.D_W_ID, d.D_ID;
