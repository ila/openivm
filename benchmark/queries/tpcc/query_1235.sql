-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, COUNT(*) AS total, COUNT(C_MIDDLE) AS with_mid, COUNT(*) - COUNT(C_MIDDLE) AS without_mid FROM CUSTOMER GROUP BY C_W_ID;
