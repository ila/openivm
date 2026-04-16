-- {"operators": "INNER_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT i.I_ID, agg.avg_val FROM ITEM i JOIN (SELECT S_I_ID, AVG(S_W_ID) AS avg_val FROM STOCK GROUP BY S_I_ID) agg ON i.I_ID = agg.S_I_ID;
