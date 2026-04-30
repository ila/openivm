-- {"operators": "AGGREGATE,ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "openivm_verified": true}
SELECT I_ID, I_PRICE, ROW_NUMBER() OVER (ORDER BY I_PRICE DESC) AS price_rank, I_PRICE - AVG(I_PRICE) OVER () AS diff_from_avg FROM ITEM;
