-- {"operators": "AGGREGATE,WINDOW,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT gk, cnt, cnt * 1.0 / SUM(cnt) OVER () AS share FROM (SELECT C_CREDIT AS gk, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_CREDIT) sub;
