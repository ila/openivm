-- {"operators": "INNER_JOIN,FILTER,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
WITH high_stock AS (SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK WHERE S_QUANTITY > 100) SELECT i.I_ID, i.I_NAME, i.I_PRICE, hs.S_QUANTITY FROM ITEM i JOIN high_stock hs ON i.I_ID = hs.S_I_ID;
