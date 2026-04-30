-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT s.S_W_ID, COUNT(i.I_ID) AS items, SUM(s.S_QUANTITY) AS total_qty FROM ITEM i RIGHT JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY s.S_W_ID;
