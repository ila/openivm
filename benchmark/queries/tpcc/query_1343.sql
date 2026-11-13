-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "HISTORY"}
SELECT H_W_ID, H_C_ID FROM HISTORY WHERE H_AMOUNT > (SELECT AVG(H_AMOUNT) FROM HISTORY);
