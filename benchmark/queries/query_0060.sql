-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT COUNT(*) FROM CUSTOMER INNER JOIN OORDER ON CUSTOMER.C_ID = OORDER.O_C_ID;
