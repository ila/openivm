-- {"operators": "INNER_JOIN,FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "fn:CURRENT_TIMESTAMP"}
SELECT C_W_ID, C_ID, C_SINCE, DATE_DIFF('day', C_SINCE, CURRENT_TIMESTAMP) AS days_since_join FROM CUSTOMER WHERE C_SINCE IS NOT NULL;
