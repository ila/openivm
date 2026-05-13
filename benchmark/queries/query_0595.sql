-- {"operators": "FILTER,DISTINCT,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT D_W_ID, D_ID, D_NAME FROM DISTRICT WHERE D_TAX IN (SELECT DISTINCT W_TAX FROM WAREHOUSE);
