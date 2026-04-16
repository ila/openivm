-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "non_incr_reason": "op:LIMIT,ORDER"}
SELECT I_ID, I_NAME, I_PRICE FROM ITEM ORDER BY I_PRICE DESC LIMIT 20;
