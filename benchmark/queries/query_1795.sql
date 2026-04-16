-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE", "openivm_verified": true}
SELECT o.O_ID, ol.OL_NUMBER, ROW_NUMBER() OVER (PARTITION BY o.O_ID ORDER BY ol.OL_NUMBER DESC) AS rn FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID;
