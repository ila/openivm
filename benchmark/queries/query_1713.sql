-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT i.I_ID, COUNT(*) AS n, SUM(COALESCE(s.S_W_ID, 0)) AS tot, AVG(s.S_W_ID) AS avg_val FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY i.I_ID;
