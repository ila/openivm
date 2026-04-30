-- {"operators": "AGGREGATE,UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY"}
SELECT C_W_ID AS w_id, SUM(C_BALANCE) AS total FROM CUSTOMER GROUP BY C_W_ID UNION SELECT H_W_ID, SUM(H_AMOUNT) FROM HISTORY GROUP BY H_W_ID;
