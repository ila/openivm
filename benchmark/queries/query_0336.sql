-- {"operators": "AGGREGATE,UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,STOCK"}
SELECT C_W_ID, COUNT(*) FROM CUSTOMER GROUP BY C_W_ID UNION ALL SELECT S_W_ID, COUNT(*) FROM STOCK GROUP BY S_W_ID;
