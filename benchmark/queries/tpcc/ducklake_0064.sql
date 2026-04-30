-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "CUSTOMER", "ducklake": true}
SELECT C_W_ID, C_ID, C_BALANCE, CASE WHEN C_BALANCE < 0 THEN 'debt' WHEN C_BALANCE = 0 THEN 'neutral' ELSE 'credit' END AS status, ABS(C_BALANCE) AS abs_bal, (C_BALANCE + 100.0) AS adj_bal FROM dl.CUSTOMER;
