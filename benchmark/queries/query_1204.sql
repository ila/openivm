-- {"operators": "UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT W_ID AS id, W_YTD AS amount FROM WAREHOUSE UNION ALL SELECT D_W_ID * 100 + D_ID, D_YTD FROM DISTRICT;
