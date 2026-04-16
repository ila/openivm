-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "openivm_verified": true}
SELECT I_IM_ID, I_PRICE, SUM(I_PRICE) OVER (PARTITION BY I_IM_ID) AS part_sum FROM ITEM;
