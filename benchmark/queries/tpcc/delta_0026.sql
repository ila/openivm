-- {"operators": "FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER", "delta": true}
SELECT c.C_W_ID, c.C_ID, c.C_LAST AS last FROM d_CUSTOMER c WHERE c.C_ID IN (SELECT o.O_C_ID FROM d_OORDER o WHERE o.O_OL_CNT > 3);
