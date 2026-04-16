-- {"operators": "FILTER,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT DISTINCT O_CARRIER_ID FROM OORDER WHERE O_CARRIER_ID IS NOT NULL;
