-- {"operators": "SEMI_JOIN,IN,PROJECTION", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER", "openivm_verified": true}
SELECT c.C_ID + 100 AS C_ID, c.C_W_ID, c.C_D_ID FROM CUSTOMER c WHERE c.C_ID IN (SELECT o.O_C_ID FROM OORDER o WHERE o.O_W_ID = c.C_W_ID AND o.O_D_ID = c.C_D_ID);
