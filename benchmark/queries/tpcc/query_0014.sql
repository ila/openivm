-- {"operators": "FILTER,CTE,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
WITH high_value_customers AS (SELECT C_ID FROM CUSTOMER WHERE C_BALANCE > 5000) SELECT * FROM high_value_customers;
