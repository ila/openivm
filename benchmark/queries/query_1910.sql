-- {"operators": "AGGREGATE,FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT i.I_ID, i.I_NAME FROM ITEM i WHERE (SELECT SUM(s.S_QUANTITY) FROM STOCK s WHERE s.S_I_ID = i.I_ID) > 100;
