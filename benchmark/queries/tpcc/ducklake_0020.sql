-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "ducklake": true}
SELECT w.W_ID AS warehouse, w.W_NAME AS wname, d.D_ID AS district, d.D_NAME AS dname, d.D_YTD AS ytd FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID;
