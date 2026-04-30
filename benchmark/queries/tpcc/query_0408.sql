-- {"operators": "AGGREGATE,LIMIT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "ivm_type": "RECOMPUTE"}
SELECT W_ID, COUNT(*), SUM(W_YTD), AVG(W_TAX) FROM WAREHOUSE GROUP BY W_ID LIMIT 20;
