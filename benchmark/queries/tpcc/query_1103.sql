-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT i.I_ID, COUNT(*) AS cnt FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY i.I_ID;
