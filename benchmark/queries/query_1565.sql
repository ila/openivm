-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_STATE, UPPER(C_STATE) AS state_upper, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID, C_STATE;
