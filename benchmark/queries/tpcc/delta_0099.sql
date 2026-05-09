-- {"operators": "FILTER,SUBQUERY_FILTER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "non_incr_reason": "op:SUBQUERY_FILTER", "delta": true}
SELECT c.C_W_ID AS w, c.C_ID AS cid, c.C_BALANCE AS bal FROM d_CUSTOMER c WHERE (c.C_W_ID, c.C_D_ID) IN (SELECT D_W_ID, D_ID FROM d_DISTRICT WHERE D_YTD > 10000);
