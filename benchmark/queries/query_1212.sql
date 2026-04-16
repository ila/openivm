-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "OORDER"}
SELECT O_W_ID, O_D_ID, COUNT(CASE WHEN O_ALL_LOCAL = 1 THEN 1 END) AS local_cnt, COUNT(CASE WHEN O_ALL_LOCAL = 0 THEN 1 END) AS remote_cnt FROM OORDER GROUP BY O_W_ID, O_D_ID;
