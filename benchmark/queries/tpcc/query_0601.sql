-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,STOCK"}
SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK WHERE S_W_ID IN (SELECT W_ID FROM WAREHOUSE WHERE W_STATE = 'CA') OR S_QUANTITY > (SELECT MAX(S_QUANTITY) FROM STOCK) * 0.9;
