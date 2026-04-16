-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, COUNT(DISTINCT S_I_ID) as item_cnt, SUM(S_QUANTITY) as qty FROM STOCK GROUP BY S_W_ID;
