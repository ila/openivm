-- {"operators": "INNER_JOIN,AGGREGATE,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "ducklake": true}
SELECT s.S_W_ID, COUNT(*) AS items, AVG(i.I_PRICE) AS avg_price FROM dl.STOCK s JOIN dl.ITEM i ON s.S_I_ID = i.I_ID WHERE i.I_PRICE > 10 GROUP BY s.S_W_ID;
