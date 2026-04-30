-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_I_ID, OL_AMOUNT FROM ORDER_LINE WHERE OL_AMOUNT BETWEEN 10 AND 100000;
