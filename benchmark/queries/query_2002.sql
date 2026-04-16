-- {"operators": "VALUES_ONLY", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "UNKNOWN", "non_incr_reason": "op:VALUES_ONLY"}
SELECT * FROM (VALUES (1, 'GC', 'good'), (2, 'BC', 'bad')) t(n, code, label);
