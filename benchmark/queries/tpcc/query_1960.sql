-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, COUNT(*) FILTER (WHERE S_QUANTITY < 20) AS low, COUNT(*) FILTER (WHERE S_QUANTITY BETWEEN 20 AND 80) AS mid, COUNT(*) FILTER (WHERE S_QUANTITY > 80) AS high FROM STOCK GROUP BY S_W_ID;
