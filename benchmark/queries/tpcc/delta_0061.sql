-- {"operators": "FILTER", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER", "delta": true}
SELECT O_W_ID, O_D_ID, O_ID AS order_id, O_CARRIER_ID AS carrier, (O_CARRIER_ID IS NULL) AS pending FROM d_OORDER WHERE O_CARRIER_ID IS NULL;
