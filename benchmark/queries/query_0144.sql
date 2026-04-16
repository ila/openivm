-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT OORDER.O_ID, COUNT(*) FROM OORDER JOIN ORDER_LINE ON OORDER.O_ID = ORDER_LINE.OL_O_ID GROUP BY OORDER.O_ID;
