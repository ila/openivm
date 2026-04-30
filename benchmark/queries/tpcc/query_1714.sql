-- {"operators": "INNER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT i.I_ID, MIN(s.S_W_ID) AS mn, MAX(s.S_W_ID) AS mx, COUNT(DISTINCT s.S_W_ID) AS uniq FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY i.I_ID;
