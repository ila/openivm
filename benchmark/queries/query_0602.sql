-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, COUNT(*) AS cnt FROM CUSTOMER WHERE C_D_ID IN (SELECT D_ID FROM DISTRICT WHERE D_TAX < 0.05) GROUP BY C_W_ID;
