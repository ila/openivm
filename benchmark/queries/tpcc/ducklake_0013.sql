-- {"operators": "INNER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK", "ducklake": true}
SELECT s.S_W_ID, COUNT(DISTINCT i.I_ID) AS distinct_items, SUM(s.S_QUANTITY) AS total_qty FROM dl.STOCK s JOIN dl.ITEM i ON s.S_I_ID = i.I_ID GROUP BY s.S_W_ID;
