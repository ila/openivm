-- {"operators": "AGGREGATE,SUBQUERY_FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, I_NAME, I_PRICE, (SELECT AVG(I_PRICE) FROM ITEM) AS overall_avg FROM ITEM;
