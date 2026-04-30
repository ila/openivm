-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_IM_ID, AVG(I_PRICE) AS avg_val FROM ITEM GROUP BY I_IM_ID;
