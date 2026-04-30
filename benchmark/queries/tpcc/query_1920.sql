-- {"operators": "CROSS_JOIN,TABLE_FUNCTION,SUBQUERY,VALUES_ONLY", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "non_incr_reason": "op:VALUES_ONLY"}
SELECT s.label, w.W_ID, w.W_NAME FROM WAREHOUSE w CROSS JOIN (SELECT unnest(['small', 'medium', 'large']) AS label) s;
