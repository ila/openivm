-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "ducklake": true}
SELECT c.C_W_ID, c.C_ID, c.C_BALANCE, d.D_YTD, COALESCE(c.C_BALANCE / NULLIF(d.D_YTD, 0), 0) AS share FROM dl.CUSTOMER c JOIN dl.DISTRICT d ON c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID;
