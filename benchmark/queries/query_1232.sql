-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_ID, O_W_ID, O_CARRIER_ID, COALESCE(O_CARRIER_ID, -1) AS safe_carrier FROM OORDER;
