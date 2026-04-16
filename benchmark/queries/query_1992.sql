-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, COUNT(*) AS total, COUNT(C_MIDDLE) AS with_middle, COUNT(DISTINCT C_CREDIT) AS credit_types, COUNT(DISTINCT C_STATE) AS states FROM CUSTOMER GROUP BY C_W_ID;
