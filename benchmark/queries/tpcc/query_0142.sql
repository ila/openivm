-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT * FROM OORDER INNER JOIN ORDER_LINE ON OORDER.O_ID = ORDER_LINE.OL_O_ID;
