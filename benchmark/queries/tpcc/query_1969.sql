-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, SUM(CAST(C_PAYMENT_CNT AS BIGINT)) AS total_payments, SUM(CAST(C_DELIVERY_CNT AS BIGINT)) AS total_deliveries FROM CUSTOMER GROUP BY C_W_ID;
