-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "openivm_verified": true}
SELECT I_IM_ID, I_PRICE, LAG(I_PRICE, 1) OVER (PARTITION BY I_IM_ID ORDER BY I_PRICE) AS prev FROM ITEM;
