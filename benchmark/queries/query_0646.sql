-- {"operators": "ORDER,DISTINCT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "op:ORDER"}
SELECT DISTINCT C_STATE FROM CUSTOMER ORDER BY C_STATE;
