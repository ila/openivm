-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_ID, W_YTD, W_TAX, W_YTD * W_TAX AS tax_amt, W_YTD / NULLIF(W_TAX, 0) AS inv_tax FROM WAREHOUSE;
