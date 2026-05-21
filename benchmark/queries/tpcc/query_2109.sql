-- {"operators": "PIVOT,AGGREGATE,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT p.*, ROW_NUMBER() OVER (ORDER BY S_I_ID) AS pivot_rank FROM (PIVOT STOCK ON S_W_ID IN (1, 2, 3) USING AVG(S_QUANTITY) GROUP BY S_I_ID) p;
