-- {"operators": "AGGREGATE,UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY", "delta": true}
SELECT C_W_ID AS w, COUNT(*) AS n, 'CUSTOMER' AS src FROM d_CUSTOMER GROUP BY C_W_ID UNION ALL SELECT H_W_ID AS w, COUNT(*) AS n, 'HISTORY' AS src FROM d_HISTORY GROUP BY H_W_ID;
