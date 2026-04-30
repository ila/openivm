-- {"operators": "INNER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT", "ducklake": true}
SELECT d1.D_W_ID AS wid, d1.D_ID AS district_1, d2.D_ID AS district_2, (d1.D_YTD + d2.D_YTD) AS combined_ytd FROM dl.DISTRICT d1 JOIN dl.DISTRICT d2 ON d1.D_W_ID = d2.D_W_ID AND d1.D_ID < d2.D_ID;
