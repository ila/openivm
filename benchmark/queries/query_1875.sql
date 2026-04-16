-- {"operators": "AGGREGATE,ORDER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, LIST(C_ID ORDER BY C_BALANCE DESC) AS cust_list FROM CUSTOMER GROUP BY C_W_ID;
