-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "CUSTOMER"}
SELECT C_ID, CASE WHEN C_BALANCE < -1000 THEN 'very_negative' WHEN C_BALANCE < 0 THEN 'negative' WHEN C_BALANCE = 0 THEN 'zero' ELSE 'positive' END FROM CUSTOMER;
