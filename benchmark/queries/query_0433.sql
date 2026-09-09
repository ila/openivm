-- {"operators": "AGGREGATE,FILTER,LIMIT,HAVING", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "mv_verified": true, "refresh_type": "FULL_REFRESH"}
SELECT W_ID, MIN(W_TAX) FROM WAREHOUSE WHERE W_TAX > 1 GROUP BY W_ID HAVING MIN(W_TAX) < 0 LIMIT 1;
