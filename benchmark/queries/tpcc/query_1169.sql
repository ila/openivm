-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT COUNT(DISTINCT O_CARRIER_ID) AS uniq FROM OORDER;
