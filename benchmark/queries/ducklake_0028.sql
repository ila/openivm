-- {"operators": "FILTER,VALUES_ONLY", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "UNKNOWN", "non_incr_reason": "op:VALUES_ONLY", "ducklake": true}
SELECT x AS num, y AS letter FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(x, y) WHERE x > 1;
