-- {"operators": "AGGREGATE,FILTER,UNION", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID AS w_id, COUNT(*) AS cnt FROM CUSTOMER WHERE C_CREDIT = 'GC' GROUP BY C_W_ID UNION SELECT C_W_ID, COUNT(*) FROM CUSTOMER WHERE C_CREDIT = 'BC' GROUP BY C_W_ID;
