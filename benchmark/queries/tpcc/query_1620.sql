-- {"operators": "INNER_JOIN,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT DISTINCT c.C_CREDIT, o.O_ALL_LOCAL FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID;
