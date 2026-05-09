-- {"operators": "FILTER,CTE,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "delta": true}
WITH high_value_customers AS (SELECT C_ID FROM d_CUSTOMER WHERE C_BALANCE > 5000) SELECT * FROM high_value_customers;
