-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_I_ID, OL_AMOUNT FROM ORDER_LINE WHERE OL_AMOUNT >= 10 AND OL_AMOUNT IS NOT NULL;
