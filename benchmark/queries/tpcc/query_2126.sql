-- {"operators": "POSITIONAL_JOIN,UNNEST,TABLE_FUNCTION", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,ITEM", "non_incr_reason": "join:POSITIONAL"}
SELECT s.S_W_ID, i.I_ID, u.label FROM STOCK s POSITIONAL JOIN ITEM i CROSS JOIN UNNEST(['left_pos', 'right_pos']) AS u(label);
