-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT COUNT(*) AS cnt, SUM(I_PRICE) AS total, AVG(I_PRICE) AS avg_val FROM ITEM;
