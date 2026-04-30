-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": true, "tables": "CUSTOMER"}
SELECT C_W_ID, C_ID, C_BALANCE, CASE WHEN C_BALANCE IS NULL THEN 0.0 ELSE C_BALANCE END AS safe_balance FROM CUSTOMER;
