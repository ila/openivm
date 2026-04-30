-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "ivm_type": "RECOMPUTE"}
SELECT C_W_ID, C_ID, C_LAST FROM CUSTOMER ORDER BY C_W_ID, C_ID LIMIT 50;
