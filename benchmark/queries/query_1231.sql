-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, POWER(I_PRICE, 2) AS sq_price, SQRT(I_PRICE) AS sqrt_price, LN(I_PRICE + 1) AS ln_price FROM ITEM WHERE I_PRICE > 0;
