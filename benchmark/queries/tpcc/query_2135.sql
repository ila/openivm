-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "openivm_verified": true}
SELECT OL_W_ID, OL_D_ID, COUNT(*) AS line_count, SUM(OL_AMOUNT) AS total_amount FROM ORDER_LINE GROUP BY OL_W_ID, OL_D_ID;
