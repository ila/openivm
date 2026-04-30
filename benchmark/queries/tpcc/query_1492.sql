-- {"operators": "INNER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT i.I_ID, i.I_NAME, i.I_PRICE, SUM(s.S_QUANTITY) AS stock, COUNT(DISTINCT s.S_W_ID) AS in_wh FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY i.I_ID, i.I_NAME, i.I_PRICE;
