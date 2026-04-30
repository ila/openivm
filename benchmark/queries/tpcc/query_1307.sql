-- {"operators": "AGGREGATE,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
WITH stats AS (SELECT C_W_ID, AVG(C_BALANCE) AS avg_b, STDDEV(C_BALANCE) AS std_b FROM CUSTOMER GROUP BY C_W_ID) SELECT C_W_ID, avg_b, std_b, ROUND(avg_b / NULLIF(std_b, 0), 4) AS cv FROM stats;
