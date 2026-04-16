-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "openivm_verified": true}
SELECT I_IM_ID, I_PRICE, ROW_NUMBER() OVER (PARTITION BY I_IM_ID ORDER BY I_PRICE DESC) AS rn FROM ITEM;
