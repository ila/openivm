-- {"operators": "FILTER,TABLE_FUNCTION", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT s.S_W_ID, s.S_I_ID, dist.n AS district_slot FROM STOCK s, generate_series(1, 10) dist(n) WHERE s.S_W_ID * 10 + dist.n <= 100;
