-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT COUNT(*) FROM OORDER WHERE O_CARRIER_ID IS NULL;
