-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT w1.W_ID AS w1, w2.W_ID AS w2 FROM WAREHOUSE w1 JOIN WAREHOUSE w2 ON w1.W_STATE = w2.W_STATE AND w1.W_ID < w2.W_ID;
