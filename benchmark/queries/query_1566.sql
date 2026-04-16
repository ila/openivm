-- {"operators": "LIMIT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "non_incr_reason": "op:LIMIT"}
SELECT I_ID, I_NAME, REGEXP_MATCHES(I_NAME, '[A-Z]+') AS matches FROM ITEM LIMIT 50;
