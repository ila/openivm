-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_D_ID, O_OL_CNT FROM OORDER WHERE O_OL_CNT >= 5 AND O_OL_CNT IS NOT NULL;
