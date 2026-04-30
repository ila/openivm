-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "openivm_verified": true}
SELECT w.W_ID, d.D_ID, d.D_YTD, d.D_YTD - LAG(d.D_YTD) OVER (PARTITION BY w.W_ID ORDER BY d.D_ID) AS diff_prev FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID;
