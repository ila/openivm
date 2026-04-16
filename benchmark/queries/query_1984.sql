-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT c.C_W_ID, c.C_ID, LEAST(c.C_BALANCE, c.C_CREDIT_LIM) AS effective_limit FROM CUSTOMER c;
