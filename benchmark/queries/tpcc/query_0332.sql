-- {"operators": "AGGREGATE,FILTER,DISTINCT,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT COUNT(*) FROM (SELECT DISTINCT O_W_ID FROM OORDER WHERE O_ALL_LOCAL = 1);
