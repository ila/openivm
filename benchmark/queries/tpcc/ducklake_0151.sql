-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "ducklake": true}
SELECT w.W_ID, d.D_ID, (d.D_YTD + d.D_TAX * 1000) AS adj_ytd FROM dl.WAREHOUSE w JOIN dl.DISTRICT d ON w.W_ID = d.D_W_ID;
