-- {"operators": "AGGREGATE,ORDER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, LIST(C_LAST ORDER BY C_BALANCE DESC) AS top_names FROM CUSTOMER GROUP BY C_W_ID;
