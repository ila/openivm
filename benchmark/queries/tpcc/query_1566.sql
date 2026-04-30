-- {"operators": "LIMIT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "ivm_type": "RECOMPUTE"}
SELECT I_ID, I_NAME, REGEXP_MATCHES(I_NAME, '[A-Z]+') AS matches FROM ITEM LIMIT 50;
