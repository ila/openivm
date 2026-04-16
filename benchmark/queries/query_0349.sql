-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT W_ID, COUNT(*) FILTER (WHERE W_TAX > 0.05) as high_tax_cnt, COUNT(*) FILTER (WHERE W_YTD > 10000) as high_ytd_cnt FROM WAREHOUSE GROUP BY W_ID;
