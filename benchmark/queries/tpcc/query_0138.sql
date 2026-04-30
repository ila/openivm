-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER"}
SELECT DISTRICT.D_W_ID, COUNT(*) FROM DISTRICT JOIN CUSTOMER ON DISTRICT.D_W_ID = CUSTOMER.C_W_ID GROUP BY DISTRICT.D_W_ID;
