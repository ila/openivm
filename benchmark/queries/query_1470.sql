-- {"operators": "CROSS_JOIN,FILTER,TABLE_FUNCTION,SUBQUERY,VALUES_ONLY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT c.C_W_ID, u.v AS creditview FROM CUSTOMER c CROSS JOIN (SELECT unnest(['BC', 'GC']) AS v) u WHERE c.C_CREDIT = u.v;
