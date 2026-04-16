-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT * FROM WAREHOUSE INNER JOIN CUSTOMER ON WAREHOUSE.W_ID = CUSTOMER.C_W_ID;
