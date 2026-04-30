-- {"operators": "CROSS_JOIN,FILTER,TABLE_FUNCTION", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT c.C_W_ID, r.n FROM CUSTOMER c CROSS JOIN range(1, 4) r(n) WHERE c.C_BALANCE > r.n * 1000;
