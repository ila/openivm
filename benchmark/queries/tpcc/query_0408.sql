-- {"operators": "AGGREGATE,LIMIT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "refresh_type": "FULL_REFRESH", "mv_verified": true}
SELECT W_ID, COUNT(*), SUM(W_YTD), AVG(W_TAX) FROM WAREHOUSE GROUP BY W_ID LIMIT 20;
