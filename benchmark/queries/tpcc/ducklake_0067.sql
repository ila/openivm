-- {"operators": "UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY", "ducklake": true}
SELECT C_W_ID AS w, C_D_ID AS d, C_ID AS id, C_BALANCE AS amt, 'balance' AS kind FROM CUSTOMER UNION ALL SELECT H_W_ID AS w, H_D_ID AS d, H_C_ID AS id, H_AMOUNT AS amt, 'payment' AS kind FROM HISTORY;
