-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_ID, O_W_ID, IFNULL(O_CARRIER_ID, 0) AS carrier FROM OORDER;
