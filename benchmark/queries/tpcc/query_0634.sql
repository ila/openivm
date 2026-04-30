-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_ID, O_ENTRY_D, EXTRACT(YEAR FROM O_ENTRY_D) AS yr, EXTRACT(MONTH FROM O_ENTRY_D) AS mo, EXTRACT(DOW FROM O_ENTRY_D) AS dow, DATE_TRUNC('week', O_ENTRY_D) AS week_start FROM OORDER WHERE O_ENTRY_D IS NOT NULL;
