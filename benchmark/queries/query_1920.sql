-- {"operators": "CROSS_JOIN,TABLE_FUNCTION,SUBQUERY,VALUES_ONLY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT s.label, w.W_ID, w.W_NAME FROM WAREHOUSE w CROSS JOIN (SELECT unnest(['small', 'medium', 'large']) AS label) s;
