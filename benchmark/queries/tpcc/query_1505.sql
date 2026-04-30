-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT o.O_W_ID, o.O_D_ID, COUNT(c.C_ID) AS cust_matched, COUNT(*) AS total_orders FROM CUSTOMER c RIGHT JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY o.O_W_ID, o.O_D_ID;
