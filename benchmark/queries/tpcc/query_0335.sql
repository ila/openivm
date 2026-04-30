-- {"operators": "UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT,CUSTOMER"}
SELECT W_ID as id, 'warehouse' as type FROM WAREHOUSE UNION SELECT D_W_ID, 'district' FROM DISTRICT UNION SELECT C_W_ID, 'customer' FROM CUSTOMER;
