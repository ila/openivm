-- {"operators": "INNER_JOIN,AGGREGATE,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE", "openivm_verified": true}
SELECT i.I_ID, ol.OL_W_ID, SUM(ol.OL_W_ID) OVER (PARTITION BY i.I_ID) AS part_total FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID;
