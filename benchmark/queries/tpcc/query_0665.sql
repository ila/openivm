-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": true, "has_case": true, "tables": "OORDER"}
SELECT O_ID, O_W_ID, COALESCE(O_CARRIER_ID, -1) AS carrier_id, CASE WHEN O_CARRIER_ID IS NULL THEN 'unassigned' ELSE CAST(O_CARRIER_ID AS VARCHAR) END AS carrier_str FROM OORDER;
