-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE"}
SELECT W_STATE, COUNT(DISTINCT W_YTD) AS unique_vals FROM WAREHOUSE GROUP BY W_STATE;
