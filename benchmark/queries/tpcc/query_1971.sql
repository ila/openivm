-- {"operators": "AGGREGATE,FILTER,SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY", "non_incr_reason": "kw:ALL ("}
SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE > ALL (SELECT AVG(H_AMOUNT) FROM HISTORY GROUP BY H_W_ID);
