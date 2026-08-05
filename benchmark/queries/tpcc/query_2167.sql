-- {"operators": "TOP_K,ORDER_BY,LIMIT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_D_ID, C_ID, C_BALANCE FROM CUSTOMER ORDER BY C_BALANCE DESC, C_W_ID, C_D_ID, C_ID LIMIT 25;
