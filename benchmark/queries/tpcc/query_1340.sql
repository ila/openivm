-- {"operators": "FILTER,DISTINCT,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT C_W_ID, C_ID FROM CUSTOMER WHERE C_ID IN (SELECT DISTINCT O_C_ID FROM OORDER WHERE O_OL_CNT > 10);
