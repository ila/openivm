-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "WAREHOUSE"}
SELECT W_ID, W_NAME, W_YTD, W_TAX, ROUND(W_YTD * (1 - W_TAX), 2) AS net_ytd, CASE WHEN W_TAX > 0.1 THEN 'high_tax' ELSE 'low_tax' END AS tax_band FROM WAREHOUSE;
