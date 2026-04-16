-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "openivm_verified": true}
SELECT O_W_ID, O_OL_CNT, ROW_NUMBER() OVER (PARTITION BY O_W_ID ORDER BY O_OL_CNT DESC) AS rn FROM OORDER;
