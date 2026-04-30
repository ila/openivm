-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT S_W_ID, COUNT(*) AS cnt, SUM(S_QUANTITY) AS total FROM STOCK WHERE S_QUANTITY > 50 GROUP BY S_W_ID;
