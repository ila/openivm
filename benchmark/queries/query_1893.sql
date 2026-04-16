-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK"}
SELECT label, COUNT(s.S_W_ID) AS cnt FROM (VALUES ('low', 0, 20), ('med', 20, 100), ('high', 100, 99999)) AS t(label, lo, hi) LEFT JOIN STOCK s ON s.S_QUANTITY >= t.lo AND s.S_QUANTITY < t.hi GROUP BY label;
