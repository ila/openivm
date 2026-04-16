-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT c1.C_W_ID, c1.C_ID AS c1_id, c2.C_ID AS c2_id, c1.C_BALANCE AS b1, c2.C_BALANCE AS b2 FROM CUSTOMER c1 JOIN CUSTOMER c2 ON c1.C_W_ID = c2.C_W_ID AND c1.C_ID < c2.C_ID AND ABS(c1.C_BALANCE - c2.C_BALANCE) < 10;
