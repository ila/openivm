-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT EXTRACT(YEAR FROM C_SINCE) as year_since, EXTRACT(MONTH FROM C_SINCE) as month_since FROM CUSTOMER;
