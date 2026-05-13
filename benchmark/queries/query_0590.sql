-- {"operators": "FILTER,SUBQUERY_FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT C_ID, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID IN (SELECT W_ID FROM WAREHOUSE WHERE W_TAX > 0.1);
