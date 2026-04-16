-- {"operators": "CROSS_JOIN,FILTER,TABLE_FUNCTION", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "openivm_verified": true}
SELECT i.I_ID, s.slot FROM ITEM i CROSS JOIN generate_series(1, 3) s(slot) WHERE i.I_PRICE > s.slot * 20;
