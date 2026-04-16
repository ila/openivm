-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "openivm_verified": true}
SELECT O_W_ID, O_D_ID, COUNT(*) AS orders FROM OORDER GROUP BY CUBE(O_W_ID, O_D_ID);
