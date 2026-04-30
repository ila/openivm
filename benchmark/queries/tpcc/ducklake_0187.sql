-- {"operators": "AGGREGATE,UNION,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "ducklake": true}
WITH a AS (SELECT D_W_ID AS w, COUNT(*) AS n FROM dl.DISTRICT GROUP BY D_W_ID), b AS (SELECT C_W_ID AS w, COUNT(*) AS n FROM dl.CUSTOMER GROUP BY C_W_ID) SELECT * FROM a UNION SELECT * FROM b;
