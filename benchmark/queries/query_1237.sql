-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": true, "tables": "OORDER"}
SELECT O_W_ID, O_D_ID, COUNT(*) AS total, SUM(CASE WHEN O_CARRIER_ID IS NULL THEN 1 ELSE 0 END) AS unassigned FROM OORDER GROUP BY O_W_ID, O_D_ID;
