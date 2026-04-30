-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT c.C_W_ID, SUM(c.C_BALANCE) as total_balance, COUNT(o.O_ID) as order_count FROM CUSTOMER c LEFT JOIN OORDER o ON c.C_ID = o.O_C_ID GROUP BY c.C_W_ID;
