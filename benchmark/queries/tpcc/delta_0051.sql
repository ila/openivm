-- {"operators": "FILTER,UNION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY", "delta": true}
SELECT C_W_ID, C_D_ID FROM d_CUSTOMER WHERE C_CREDIT = 'GC' UNION SELECT H_W_ID, H_D_ID FROM d_HISTORY WHERE H_AMOUNT > 50;
