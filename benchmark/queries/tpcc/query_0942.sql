-- {"operators": "AGGREGATE,HAVING", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_IM_ID, COUNT(*) AS cnt, AVG(I_PRICE) AS avg_val FROM ITEM GROUP BY I_IM_ID HAVING COUNT(*) > 0 AND AVG(I_PRICE) > 0;
