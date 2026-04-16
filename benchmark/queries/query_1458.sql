-- {"operators": "CROSS_JOIN,TABLE_FUNCTION", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT", "openivm_verified": true}
SELECT d.D_W_ID, d.D_ID, m.mo FROM DISTRICT d CROSS JOIN generate_series(1, 12) m(mo);
