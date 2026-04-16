-- {"operators": "UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT W_ID AS id, 'warehouse' AS type FROM WAREHOUSE UNION SELECT C_W_ID, 'customer' FROM CUSTOMER;
