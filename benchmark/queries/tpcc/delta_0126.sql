-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "delta": true}
SELECT v.label, COUNT(*) AS n FROM d_CUSTOMER c JOIN (VALUES ('premium', 0), ('standard', -100), ('debt', -999999)) AS v(label, threshold) ON c.C_BALANCE >= v.threshold GROUP BY v.label;
