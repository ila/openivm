-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT W_ID, W_NAME, W_YTD FROM WAREHOUSE WHERE W_YTD > (SELECT AVG(D_YTD) FROM DISTRICT);
