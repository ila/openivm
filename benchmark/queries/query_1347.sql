-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER"}
SELECT d.D_W_ID, d.D_ID, d.D_NAME, (SELECT COUNT(*) FROM CUSTOMER WHERE C_W_ID = d.D_W_ID AND C_D_ID = d.D_ID) AS cust_count FROM DISTRICT d;
