-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT CUSTOMER.C_ID, COUNT(*) FROM CUSTOMER LEFT JOIN OORDER ON CUSTOMER.C_ID = OORDER.O_C_ID GROUP BY CUSTOMER.C_ID;
