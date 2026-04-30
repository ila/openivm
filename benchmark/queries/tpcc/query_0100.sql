-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT COUNT(*), COUNT(DISTINCT S_QUANTITY), SUM(S_QUANTITY), AVG(S_QUANTITY), MIN(S_QUANTITY), MAX(S_QUANTITY) FROM STOCK;
