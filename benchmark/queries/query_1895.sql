-- {"operators": "VALUES_ONLY", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "UNKNOWN", "non_incr_reason": "op:VALUES_ONLY"}
SELECT * FROM (VALUES (1, 'Jan'), (2, 'Feb'), (3, 'Mar'), (4, 'Apr'), (5, 'May'), (6, 'Jun')) AS months(num, name);
