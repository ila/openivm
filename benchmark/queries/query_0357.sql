-- {"operators": "AGGREGATE,UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,STOCK,OORDER"}
SELECT C_W_ID, COUNT(*) as cnt, 'customer' as type FROM CUSTOMER GROUP BY C_W_ID UNION ALL SELECT S_W_ID, COUNT(*), 'stock' FROM STOCK GROUP BY S_W_ID UNION ALL SELECT O_W_ID, COUNT(*), 'order' FROM OORDER GROUP BY O_W_ID;
