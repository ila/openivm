-- {"operators": "ORDER,LIMIT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT * FROM CUSTOMER ORDER BY C_W_ID ASC LIMIT 5;
