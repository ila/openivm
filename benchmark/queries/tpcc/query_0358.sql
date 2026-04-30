-- {"operators": "AGGREGATE,UNION,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT W_ID, COUNT(*) as d_count, 'district' as type FROM (SELECT D_W_ID as W_ID FROM DISTRICT) GROUP BY W_ID UNION ALL SELECT W_ID, COUNT(*), 'warehouse' FROM WAREHOUSE GROUP BY W_ID;
