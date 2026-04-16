-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_IM_ID, MAX(I_PRICE) AS max_val FROM ITEM GROUP BY I_IM_ID;
