-- {"operators": "SEMI_JOIN,EXISTS", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY", "openivm_verified": true}
SELECT c.C_W_ID, c.C_D_ID, c.C_ID FROM CUSTOMER c WHERE EXISTS (SELECT 1 FROM HISTORY h WHERE h.H_C_W_ID = c.C_W_ID AND h.H_C_D_ID = c.C_D_ID AND h.H_C_ID = c.C_ID);
