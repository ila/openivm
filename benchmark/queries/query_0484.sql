-- {"operators": "LIMIT,DISTINCT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "non_incr_reason": "op:LIMIT"}
SELECT DISTINCT W_ID, W_TAX FROM WAREHOUSE LIMIT 10;
