-- {"operators": "UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE", "delta": true}
SELECT O_W_ID AS wid, O_D_ID AS did, 'order' AS kind FROM d_OORDER UNION ALL SELECT OL_W_ID AS wid, OL_D_ID AS did, 'line' AS kind FROM d_ORDER_LINE;
