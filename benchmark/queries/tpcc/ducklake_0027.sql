-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "ducklake": true}
SELECT c.C_W_ID, c.C_ID, c.C_LAST, v.priority FROM dl.CUSTOMER c JOIN (VALUES ('GC', 1), ('BC', 2)) AS v(credit, priority) ON c.C_CREDIT = v.credit;
