-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT i1.I_ID, i2.I_ID, i1.I_IM_ID FROM ITEM i1 JOIN ITEM i2 ON i1.I_IM_ID = i2.I_IM_ID AND i1.I_ID < i2.I_ID AND ABS(i1.I_PRICE - i2.I_PRICE) < 5;
