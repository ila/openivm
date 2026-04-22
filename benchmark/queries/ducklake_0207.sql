-- {"operators": "CROSS_JOIN,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "ducklake": true}
SELECT w.W_ID, v.label FROM dl.WAREHOUSE w CROSS JOIN (VALUES ('A'), ('B'), ('C')) AS v(label) WHERE w.W_YTD > 0;
