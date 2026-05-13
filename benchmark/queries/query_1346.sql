-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT W_ID, W_NAME, (SELECT COUNT(*) FROM DISTRICT WHERE D_W_ID = W_ID) AS district_count FROM WAREHOUSE;
