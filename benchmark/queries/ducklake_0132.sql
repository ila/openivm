-- {"operators": "DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "ITEM", "ducklake": true}
SELECT DISTINCT I_PRICE AS price, (I_PRICE * 1.08) AS taxed, CAST(I_PRICE AS INT) AS price_int FROM dl.ITEM;
