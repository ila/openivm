-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "CUSTOMER"}
SELECT C_W_ID, C_ID, CASE WHEN C_BALANCE > 5000 THEN 'vip' WHEN C_BALANCE > 1000 THEN 'standard' ELSE 'basic' END AS tier FROM CUSTOMER;
