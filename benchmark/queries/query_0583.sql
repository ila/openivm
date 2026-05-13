-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_ID, w.W_NAME FROM WAREHOUSE w WHERE (SELECT COUNT(*) FROM CUSTOMER c WHERE c.C_W_ID = w.W_ID AND c.C_BALANCE < 0) > 0;
