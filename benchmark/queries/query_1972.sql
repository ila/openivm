-- {"operators": "FILTER,SUBQUERY", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "non_incr_reason": "kw:ANY ("}
SELECT O_W_ID, O_ID FROM OORDER WHERE O_OL_CNT >= ANY (SELECT O_OL_CNT FROM OORDER WHERE O_ALL_LOCAL = 0);
