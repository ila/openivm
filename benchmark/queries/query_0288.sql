-- {"operators": "ORDER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "non_incr_reason": "op:ORDER"}
SELECT * FROM ORDER_LINE ORDER BY OL_AMOUNT;
