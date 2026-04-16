-- {"operators": "FULL_OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT COALESCE(i.I_ID, s.S_W_ID) AS gk, COUNT(*) AS n, SUM(COALESCE(s.S_W_ID, 0)) AS tot FROM ITEM i FULL OUTER JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY COALESCE(i.I_ID, s.S_W_ID);
