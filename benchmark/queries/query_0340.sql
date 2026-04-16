-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_D_ID, C_CREDIT, COUNT(*) as cnt, SUM(C_BALANCE) as total FROM CUSTOMER GROUP BY C_W_ID, C_D_ID, C_CREDIT;
