-- {"operators": "FILTER,ORDER,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT DISTINCT O_W_ID, O_D_ID FROM OORDER WHERE O_ALL_LOCAL = 1 ORDER BY O_W_ID;
