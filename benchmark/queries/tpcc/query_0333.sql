-- {"operators": "FILTER,SUBQUERY", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT * FROM (SELECT * FROM (SELECT C_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE < 0) WHERE C_ID > 100);
