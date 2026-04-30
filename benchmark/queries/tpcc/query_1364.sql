-- {"operators": "CROSS_JOIN,AGGREGATE,TABLE_FUNCTION,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT t.t AS tier, COUNT(*) FROM (SELECT unnest(['premium', 'standard', 'basic']) AS t) t CROSS JOIN CUSTOMER c GROUP BY t.t;
