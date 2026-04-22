-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "ducklake": true}
SELECT s1.S_W_ID, s1.S_I_ID AS item1, s2.S_I_ID AS item2 FROM dl.STOCK s1 JOIN dl.STOCK s2 ON s1.S_W_ID = s2.S_W_ID AND s1.S_QUANTITY = s2.S_QUANTITY AND s1.S_I_ID < s2.S_I_ID;
