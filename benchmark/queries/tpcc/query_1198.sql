-- {"operators": "FILTER,UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,ORDER_LINE"}
SELECT S_W_ID AS w_id, S_I_ID AS id FROM STOCK WHERE S_QUANTITY < 10 UNION ALL SELECT OL_W_ID, OL_I_ID FROM ORDER_LINE WHERE OL_AMOUNT > 100;
