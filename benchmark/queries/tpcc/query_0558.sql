-- {"operators": "OUTER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,HISTORY"}
SELECT w.W_ID, w.W_YTD, h.H_C_ID, h.H_AMOUNT FROM WAREHOUSE w RIGHT JOIN HISTORY h ON w.W_ID = h.H_W_ID;
