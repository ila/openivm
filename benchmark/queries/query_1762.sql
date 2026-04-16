-- {"operators": "INNER_JOIN,AGGREGATE,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "openivm_verified": true}
SELECT w.W_ID, c.C_ID, AVG(c.C_ID) OVER (PARTITION BY w.W_ID) AS part_avg, c.C_ID - AVG(c.C_ID) OVER (PARTITION BY w.W_ID) AS diff FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID;
