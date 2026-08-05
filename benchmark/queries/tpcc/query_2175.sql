-- {"operators": "AGGREGATE,LIST", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, LIST(C_ID ORDER BY C_ID) AS customer_ids FROM CUSTOMER GROUP BY C_W_ID;
