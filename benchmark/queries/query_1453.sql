-- {"operators": "FILTER,SUBQUERY_FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER,OORDER"}
SELECT C_W_ID, C_ID FROM CUSTOMER WHERE C_ID IN (SELECT O_C_ID FROM OORDER WHERE O_W_ID IN (SELECT W_ID FROM WAREHOUSE WHERE W_TAX > 0.05));
