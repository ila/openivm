-- {"operators": "UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER,HISTORY", "ducklake": true}
SELECT D_W_ID AS w, D_ID AS d, D_YTD AS amt FROM dl.DISTRICT UNION ALL SELECT C_W_ID AS w, C_D_ID AS d, C_BALANCE AS amt FROM dl.CUSTOMER UNION ALL SELECT H_W_ID AS w, H_D_ID AS d, H_AMOUNT AS amt FROM dl.HISTORY;
