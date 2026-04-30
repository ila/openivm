-- {"operators": "TABLE_FUNCTION,VALUES_ONLY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "UNKNOWN", "non_incr_reason": "op:VALUES_ONLY"}
SELECT unnest(list_value(1, 2, 3, 4, 5)) AS n;
