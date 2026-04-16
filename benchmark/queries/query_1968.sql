-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM", "openivm_verified": true}
SELECT I_IM_ID, COUNT(*) AS total, COUNT(*) FILTER (WHERE I_NAME LIKE '%SPECIAL%') AS specials FROM ITEM GROUP BY I_IM_ID;
