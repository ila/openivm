-- {"operators": "FULL_OUTER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT w.W_ID, w.W_NAME, o.O_ID, o.O_OL_CNT FROM WAREHOUSE w FULL OUTER JOIN OORDER o ON w.W_ID = o.O_W_ID;
