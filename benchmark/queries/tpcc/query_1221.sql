-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_ID, CAST(W_YTD AS BIGINT) AS ytd_bigint, CAST(W_TAX AS DOUBLE) AS tax_double FROM WAREHOUSE;
