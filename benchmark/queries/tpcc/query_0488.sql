-- {"operators": "LIMIT,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "ivm_type": "RECOMPUTE"}
SELECT DISTINCT OL_W_ID FROM ORDER_LINE LIMIT 10;
