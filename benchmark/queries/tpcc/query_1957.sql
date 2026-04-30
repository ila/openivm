-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "ITEM"}
SELECT CASE WHEN I_PRICE < 20 THEN 'cheap' WHEN I_PRICE < 50 THEN 'mid' ELSE 'expensive' END AS tier, COUNT(*) AS items, AVG(I_PRICE) AS avg_p FROM ITEM GROUP BY CASE WHEN I_PRICE < 20 THEN 'cheap' WHEN I_PRICE < 50 THEN 'mid' ELSE 'expensive' END;
