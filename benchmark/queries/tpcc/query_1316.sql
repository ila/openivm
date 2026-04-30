-- {"operators": "AGGREGATE,CTE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
WITH state_balance AS (SELECT C_STATE, AVG(C_BALANCE) AS avg_b, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_STATE) SELECT sb.C_STATE, sb.avg_b, sb.cnt, sb.cnt * sb.avg_b AS total FROM state_balance sb;
