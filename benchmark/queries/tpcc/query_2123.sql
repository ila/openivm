-- {"operators": "POSITIONAL_JOIN,AGGREGATE", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,ITEM", "non_incr_reason": "join:POSITIONAL"}
SELECT s.S_W_ID, COUNT(i.I_ID) AS paired_items, SUM(s.S_QUANTITY) AS paired_qty FROM STOCK s POSITIONAL JOIN ITEM i GROUP BY s.S_W_ID;
