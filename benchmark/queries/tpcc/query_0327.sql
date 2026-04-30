-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_YTD * 1.1 as ytd_110pct, W_TAX * 100 as tax_percent FROM WAREHOUSE;
