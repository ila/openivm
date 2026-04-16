-- {"operators": "INNER_JOIN,FILTER,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY, iv.I_NAME FROM STOCK s JOIN (SELECT I_ID, I_NAME FROM ITEM WHERE I_PRICE > 50) iv ON s.S_I_ID = iv.I_ID;
