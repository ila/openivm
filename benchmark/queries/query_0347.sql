-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": true, "tables": "OORDER"}
SELECT O_ID, CASE WHEN O_ALL_LOCAL = 1 AND O_CARRIER_ID IS NOT NULL THEN 'shipped_local' WHEN O_ALL_LOCAL = 1 THEN 'pending_local' WHEN O_CARRIER_ID IS NOT NULL THEN 'shipped_remote' ELSE 'pending_remote' END as status FROM OORDER;
