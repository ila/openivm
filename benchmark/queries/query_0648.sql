-- {"operators": "FILTER,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT DISTINCT OL_W_ID, OL_I_ID FROM ORDER_LINE WHERE OL_AMOUNT > 50;
