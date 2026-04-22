-- {"operators": "SCAN", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER", "non_incr_reason": "kw:EXCEPT", "ducklake": true}
SELECT C_W_ID AS w, C_D_ID AS d, C_ID AS id FROM dl.CUSTOMER EXCEPT SELECT O_W_ID AS w, O_D_ID AS d, O_C_ID AS id FROM dl.OORDER;
