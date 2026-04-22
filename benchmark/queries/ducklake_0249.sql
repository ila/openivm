-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "ducklake": true}
SELECT c.C_W_ID, c.C_D_ID, c.C_ID, c.C_LAST, c.C_FIRST, LENGTH(c.C_LAST || c.C_FIRST) AS name_len, (c.C_BALANCE + c.C_YTD_PAYMENT) AS flow FROM dl.CUSTOMER c JOIN dl.WAREHOUSE w ON c.C_W_ID = w.W_ID;
