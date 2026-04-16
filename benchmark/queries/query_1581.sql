-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT I_ID, I_PRICE, POWER(I_PRICE, 2) AS squared, SQRT(I_PRICE) AS sqrt_price, LN(I_PRICE + 1) AS ln_price, EXP(0.1) AS exp_val FROM ITEM WHERE I_PRICE > 0;
