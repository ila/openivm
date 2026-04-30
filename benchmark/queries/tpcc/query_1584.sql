-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE"}
SELECT OL_W_ID, OL_O_ID, OL_AMOUNT, ROUND(OL_AMOUNT, 0) AS rnd_0, ROUND(OL_AMOUNT, 1) AS rnd_1, ROUND(OL_AMOUNT, 3) AS rnd_3 FROM ORDER_LINE;
