-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": true, "tables": "OORDER"}
SELECT O_ID, CASE WHEN O_CARRIER_ID IS NULL THEN 'pending' WHEN O_ALL_LOCAL = 1 THEN 'local' ELSE 'remote' END FROM OORDER;
