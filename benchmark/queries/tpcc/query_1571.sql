-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_ID, DATE_PART('year', O_ENTRY_D) AS yr, DATE_PART('month', O_ENTRY_D) AS mo, DATE_PART('day', O_ENTRY_D) AS day FROM OORDER WHERE O_ENTRY_D IS NOT NULL;
