-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT * FROM ITEM INNER JOIN STOCK ON ITEM.I_ID = STOCK.S_I_ID;
