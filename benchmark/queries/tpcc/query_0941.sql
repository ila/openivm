-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_IM_ID, COUNT(*) AS cnt, SUM(I_PRICE) AS total FROM ITEM WHERE I_PRICE > 50 GROUP BY I_IM_ID;
