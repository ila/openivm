-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT WAREHOUSE.W_ID, COUNT(*) FROM WAREHOUSE LEFT JOIN CUSTOMER ON WAREHOUSE.W_ID = CUSTOMER.C_W_ID GROUP BY WAREHOUSE.W_ID;
