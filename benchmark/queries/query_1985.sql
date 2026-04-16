-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "CUSTOMER"}
SELECT C_W_ID, C_ID, C_BALANCE, C_CREDIT FROM CUSTOMER WHERE (CASE WHEN C_CREDIT = 'GC' THEN C_BALANCE > 0 WHEN C_CREDIT = 'BC' THEN C_BALANCE > 1000 ELSE FALSE END);
