-- {"operators": "AGGREGATE,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, COUNT(*) FILTER (WHERE OL_DELIVERY_D IS NULL) AS pending_lines, COUNT(*) FILTER (WHERE OL_DELIVERY_D IS NOT NULL) AS delivered_lines FROM ORDER_LINE GROUP BY OL_W_ID;
