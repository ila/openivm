-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "ducklake": true}
SELECT w.W_ID, w.W_STATE, COUNT(c.C_ID) AS cust_count, AVG(c.C_BALANCE) AS avg_bal FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_ID, w.W_STATE;
