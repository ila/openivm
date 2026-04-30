-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "CUSTOMER"}
SELECT C_W_ID, COUNT(CASE WHEN C_CREDIT = 'GC' THEN 1 END) AS good_cnt, COUNT(CASE WHEN C_CREDIT = 'BC' THEN 1 END) AS bad_cnt FROM CUSTOMER GROUP BY C_W_ID;
