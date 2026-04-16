-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_ID, LN(CAST(W_YTD AS DOUBLE) + 1) AS ln_ytd, LOG10(CAST(W_YTD AS DOUBLE) + 1) AS log10_ytd FROM WAREHOUSE WHERE W_YTD >= 0;
