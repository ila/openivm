-- {"operators": "AGGREGATE,SUBQUERY_FILTER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT I_ID, I_NAME, I_PRICE, (SELECT AVG(I_PRICE) FROM ITEM) AS overall_avg FROM ITEM;
