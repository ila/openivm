-- {"operators": "UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT O_W_ID FROM OORDER UNION SELECT OL_W_ID FROM ORDER_LINE;
