-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT ITEM.I_ID, COUNT(*) FROM ITEM JOIN STOCK ON ITEM.I_ID = STOCK.S_I_ID GROUP BY ITEM.I_ID;
