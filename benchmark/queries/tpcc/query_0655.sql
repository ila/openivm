-- {"operators": "FILTER,UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK WHERE S_QUANTITY < 10 UNION ALL SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK WHERE S_ORDER_CNT > 100;
