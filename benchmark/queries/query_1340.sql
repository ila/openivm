-- {"operators": "FILTER,DISTINCT,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER"}
SELECT C_W_ID, C_ID FROM CUSTOMER WHERE C_ID IN (SELECT DISTINCT O_C_ID FROM OORDER WHERE O_OL_CNT > 10);
