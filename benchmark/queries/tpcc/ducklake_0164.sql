-- {"operators": "FILTER,SUBQUERY", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "ducklake": true, "openivm_verified": true}
SELECT c.C_W_ID, c.C_ID FROM dl.CUSTOMER c WHERE c.C_BALANCE > ANY (SELECT C_BALANCE FROM dl.CUSTOMER WHERE C_CREDIT = 'BC');
