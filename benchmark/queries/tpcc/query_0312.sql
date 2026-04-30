-- {"operators": "OUTER_JOIN,FILTER", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT,CUSTOMER"}
SELECT * FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID WHERE w.W_TAX > 0.03 AND d.D_YTD > 1000 AND c.C_BALANCE < 100;
