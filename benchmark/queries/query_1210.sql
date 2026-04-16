-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "WAREHOUSE"}
SELECT W_ID, CASE WHEN W_TAX > 0.1 THEN 'high' ELSE 'low' END AS tax_band, W_YTD FROM WAREHOUSE;
