-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "ducklake": true}
SELECT c.C_W_ID AS wid, c.C_D_ID AS did, c.C_ID AS cid, c.C_LAST AS last_name, c.C_FIRST AS first_name, d.D_NAME AS district_name FROM dl.CUSTOMER c JOIN dl.DISTRICT d ON c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID;
