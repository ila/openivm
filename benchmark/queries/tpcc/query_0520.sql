-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_D_ID, C_ID, C_BALANCE, ROW_NUMBER() OVER (PARTITION BY C_W_ID, C_D_ID ORDER BY C_BALANCE DESC) AS rank_in_district FROM CUSTOMER;
