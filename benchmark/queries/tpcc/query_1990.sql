-- {"operators": "AGGREGATE,FILTER,UNION", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER,NEW_ORDER"}
SELECT 'active' AS type, COUNT(*) AS n FROM OORDER WHERE O_CARRIER_ID IS NULL UNION ALL SELECT 'delivered', COUNT(*) FROM OORDER WHERE O_CARRIER_ID IS NOT NULL UNION ALL SELECT 'new', COUNT(*) FROM NEW_ORDER;
