-- {"operators": "CROSS_JOIN,LATERAL,TABLE_FUNCTION,CORRELATED_SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT w_id, slot FROM WAREHOUSE w CROSS JOIN LATERAL (SELECT generate_series AS slot FROM generate_series(1, w.W_ID)) t(slot);
