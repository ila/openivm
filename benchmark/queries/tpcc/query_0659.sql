-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, COUNT(*) AS total, COUNT(DISTINCT C_STATE) AS unique_states, COUNT(DISTINCT C_CITY) AS unique_cities FROM CUSTOMER GROUP BY C_W_ID;
