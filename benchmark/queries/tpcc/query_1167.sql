-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM"}
SELECT COUNT(DISTINCT I_IM_ID) AS uniq FROM ITEM;
