-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT COUNT(*), COUNT(DISTINCT O_OL_CNT), SUM(O_OL_CNT), AVG(O_OL_CNT), MIN(O_OL_CNT), MAX(O_OL_CNT) FROM OORDER;
