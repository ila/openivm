-- {"operators": "SCAN", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER", "non_incr_reason": "kw:INTERSECT", "delta": true}
SELECT C_W_ID AS w, C_D_ID AS d FROM d_CUSTOMER INTERSECT SELECT O_W_ID AS w, O_D_ID AS d FROM d_OORDER;
