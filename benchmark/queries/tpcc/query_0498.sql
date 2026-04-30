-- {"operators": "UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,STOCK"}
SELECT C_W_ID FROM CUSTOMER UNION ALL SELECT S_W_ID FROM STOCK;
