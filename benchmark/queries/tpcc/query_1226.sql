-- {"operators": "INNER_JOIN,AGGREGATE,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, DATE_TRUNC('month', C_SINCE) AS join_month, COUNT(*) AS new_cust FROM CUSTOMER WHERE C_SINCE IS NOT NULL GROUP BY C_W_ID, DATE_TRUNC('month', C_SINCE);
