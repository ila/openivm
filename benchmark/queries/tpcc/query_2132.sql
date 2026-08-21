-- {"operators": "PROJECTION,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_D_ID, C_ID, CASE WHEN C_BALANCE < 0 THEN 'debit' WHEN C_BALANCE = 0 THEN 'zero' ELSE 'credit' END AS balance_bucket FROM CUSTOMER WHERE C_PAYMENT_CNT >= 0;
