-- {"operators": "UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT,CUSTOMER,STOCK,OORDER"}
SELECT W_ID as id FROM WAREHOUSE UNION SELECT D_W_ID FROM DISTRICT UNION SELECT C_W_ID FROM CUSTOMER UNION SELECT S_W_ID FROM STOCK UNION SELECT O_W_ID FROM OORDER;
