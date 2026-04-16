-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT COUNT(*), COUNT(DISTINCT W_TAX), SUM(W_TAX), AVG(W_TAX), MIN(W_TAX), MAX(W_TAX) FROM WAREHOUSE;
