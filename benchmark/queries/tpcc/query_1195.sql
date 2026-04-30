-- {"operators": "AGGREGATE,UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT C_W_ID AS w_id, 'customer' AS type, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID UNION SELECT O_W_ID, 'order', COUNT(*) FROM OORDER GROUP BY O_W_ID;
