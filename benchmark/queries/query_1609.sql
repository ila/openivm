-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "non_incr_reason": "fn:BOOL_AND"}
SELECT w.W_ID, BOOL_AND(d.D_YTD >= 0) AS all_non_neg, BOOL_OR(d.D_TAX > 0.1) AS any_high_tax FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID;
