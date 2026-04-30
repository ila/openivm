-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "ivm_type": "RECOMPUTE"}
SELECT * FROM OORDER ORDER BY O_ID DESC LIMIT 10;
