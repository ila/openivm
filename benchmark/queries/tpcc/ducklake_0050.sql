-- {"operators": "AGGREGATE,UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY", "ducklake": true}
SELECT C_W_ID AS w, COUNT(*) AS n, 'CUSTOMER' AS src FROM CUSTOMER GROUP BY C_W_ID UNION ALL SELECT H_W_ID AS w, COUNT(*) AS n, 'HISTORY' AS src FROM HISTORY GROUP BY H_W_ID;
