-- {"operators": "FILTER,SUBQUERY_FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT W_ID, W_NAME FROM WAREHOUSE WHERE W_ID IN (SELECT C_W_ID FROM CUSTOMER WHERE C_BALANCE > 4000);
