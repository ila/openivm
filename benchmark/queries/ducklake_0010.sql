-- {"operators": "INNER_JOIN,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER", "ducklake": true}
SELECT c.C_W_ID, c.C_D_ID, c.C_ID, c.C_LAST, o.O_ID, o.O_ENTRY_D FROM CUSTOMER c JOIN OORDER o ON c.C_W_ID = o.O_W_ID AND c.C_D_ID = o.O_D_ID AND c.C_ID = o.O_C_ID WHERE c.C_BALANCE > 0;
