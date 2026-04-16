-- {"operators": "FILTER,SUBQUERY_FILTER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT D_W_ID, D_ID, D_NAME FROM DISTRICT WHERE D_W_ID IN (SELECT W_ID FROM WAREHOUSE WHERE W_TAX > 0.08);
