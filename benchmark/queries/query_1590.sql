-- {"operators": "VALUES_ONLY", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "UNKNOWN"}
SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t(n, label);
