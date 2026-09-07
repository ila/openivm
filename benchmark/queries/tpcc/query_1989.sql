-- {"operators": "CROSS_JOIN,FILTER,LIMIT", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,ITEM", "refresh_type": "FULL_REFRESH", "mv_verified": true}
SELECT c.C_W_ID, c.C_ID, c.C_LAST, i.I_NAME FROM CUSTOMER c CROSS JOIN ITEM i WHERE c.C_LAST LIKE SUBSTRING(i.I_NAME FROM 1 FOR 1) || '%' LIMIT 100;
