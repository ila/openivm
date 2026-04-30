-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT"}
SELECT D_W_ID, D_ID, D_TAX, D_TAX * 100 AS tax_pct, ROUND(D_TAX * 100, 2) AS tax_pct_rnd FROM DISTRICT;
