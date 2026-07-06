-- {"operators": "LEFT_JOIN", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "openivm_verified": true}
SELECT w.W_ID, w.W_NAME, d.D_ID, d.D_NAME FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID AND d.D_NEXT_O_ID >= 1;
