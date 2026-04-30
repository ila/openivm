-- {"operators": "FILTER,SUBQUERY", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT * FROM (SELECT W_ID FROM WAREHOUSE WHERE W_TAX > 0.05) AS high_tax;
