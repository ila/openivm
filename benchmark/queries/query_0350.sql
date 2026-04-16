-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, COUNT(*) FILTER (WHERE C_BALANCE < 0) as negative_cnt, SUM(C_BALANCE) FILTER (WHERE C_BALANCE < 0) as negative_balance FROM CUSTOMER GROUP BY C_W_ID;
