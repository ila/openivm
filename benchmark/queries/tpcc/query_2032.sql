-- {"operators": "UNNEST,TABLE_FUNCTION,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT s.S_W_ID, s.S_I_ID, u.dist_info FROM STOCK s CROSS JOIN UNNEST([s.S_DIST_01, s.S_DIST_02, s.S_DIST_03]) AS u(dist_info) WHERE s.S_QUANTITY > 0;
