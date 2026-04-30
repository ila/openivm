-- {"operators": "AGGREGATE,DISTINCT", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER"}
SELECT O_W_ID, O_D_ID, COUNT(*) as ord_cnt, SUM(O_OL_CNT) as line_cnt, AVG(O_OL_CNT) as avg_lines, COUNT(DISTINCT O_C_ID) as cust_cnt FROM OORDER GROUP BY O_W_ID, O_D_ID;
