-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_OL_CNT FROM OORDER WHERE O_OL_CNT > 5;
