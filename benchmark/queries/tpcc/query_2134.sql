-- {"operators": "PROJECTION,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER", "openivm_verified": true}
SELECT O_W_ID, O_D_ID, O_ID, COALESCE(O_CARRIER_ID, -1) AS carrier_id FROM OORDER WHERE O_CARRIER_ID IS NULL OR O_CARRIER_ID >= 0;
