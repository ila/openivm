-- {"operators": "INNER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT COUNT(DISTINCT c.C_STATE) AS states, COUNT(DISTINCT c.C_W_ID) AS warehouses FROM CUSTOMER c JOIN WAREHOUSE w ON c.C_W_ID = w.W_ID;
