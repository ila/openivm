-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, GREATEST(w.W_YTD, COALESCE((SELECT SUM(D_YTD) FROM DISTRICT WHERE D_W_ID = w.W_ID), 0)) AS max_ytd FROM WAREHOUSE w;
