-- {"operators": "FULL_OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER"}
SELECT COALESCE(d.D_W_ID, c.C_ID) AS gk, COUNT(*) AS n, SUM(COALESCE(c.C_ID, 0)) AS tot FROM DISTRICT d FULL OUTER JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY COALESCE(d.D_W_ID, c.C_ID);
