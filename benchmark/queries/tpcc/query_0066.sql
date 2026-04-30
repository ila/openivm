-- {"operators": "AGGREGATE,DISTINCT,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT COUNT(*) FROM (SELECT DISTINCT W_ID FROM WAREHOUSE);
