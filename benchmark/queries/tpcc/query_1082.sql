-- {"operators": "INNER_JOIN,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT c.C_ID, c.C_LAST, c.C_BALANCE, o.O_ID, o.O_OL_CNT FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID WHERE c.C_ID > 0;
