-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT W_ID, W_YTD, W_TAX, W_YTD * W_TAX AS tax_amount, SUM(W_YTD * W_TAX) OVER () AS total_tax FROM WAREHOUSE;
