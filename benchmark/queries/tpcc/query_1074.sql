-- {"operators": "INNER_JOIN,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,OORDER"}
SELECT d.D_W_ID, d.D_ID, o.O_ID, o.O_OL_CNT FROM DISTRICT d JOIN OORDER o ON d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID WHERE d.D_W_ID > 0;
