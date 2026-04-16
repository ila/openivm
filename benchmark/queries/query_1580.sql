-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_ID, W_YTD, ABS(W_YTD) AS abs_ytd, SIGN(W_YTD) AS sgn, ROUND(W_YTD, 2) AS rnd FROM WAREHOUSE;
