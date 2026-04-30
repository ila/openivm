-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT * FROM CUSTOMER INNER JOIN OORDER ON CUSTOMER.C_ID = OORDER.O_C_ID;
