-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "CUSTOMER", "ducklake": true}
SELECT C_W_ID, C_ID, C_BALANCE, C_CREDIT, CASE WHEN C_BALANCE < 0 AND C_CREDIT = 'BC' THEN 'high_risk' WHEN C_BALANCE < 0 THEN 'medium_risk' ELSE 'low_risk' END AS risk, (C_BALANCE + C_CREDIT_LIM) AS available FROM dl.CUSTOMER;
