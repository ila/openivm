-- {"operators": "FILTER,ORDER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "non_incr_reason": "op:ORDER"}
SELECT * FROM WAREHOUSE WHERE W_ID > 0 ORDER BY W_ID;
