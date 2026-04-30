-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT C_W_ID, COUNT(*) AS cnt FROM CUSTOMER WHERE C_D_ID IN (SELECT D_ID FROM DISTRICT WHERE D_TAX < 0.05) GROUP BY C_W_ID;
