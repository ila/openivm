-- {"operators": "PROJECTION,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, S_I_ID, CAST(S_QUANTITY AS BIGINT) AS qty_big, CAST(S_YTD AS DOUBLE) AS ytd_double FROM STOCK WHERE S_QUANTITY BETWEEN 40 AND 100;
