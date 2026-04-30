-- {"operators": "VALUES_ONLY", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "UNKNOWN"}
SELECT * FROM (VALUES (1, 'GC', 'good'), (2, 'BC', 'bad')) t(n, code, label);
