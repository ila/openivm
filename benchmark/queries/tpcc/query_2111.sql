-- {"operators": "PIVOT,AGGREGATE,DISTINCT", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT * FROM (PIVOT (SELECT DISTINCT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK) ON S_W_ID IN (1, 2, 3) USING MAX(S_QUANTITY) GROUP BY S_I_ID) p;
