-- {"operators": "DISTINCT,SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "ducklake": true, "openivm_verified": true}
SELECT payment_count % 2 AS parity
FROM (
    SELECT distinct_payment_count AS payment_count
    FROM (
        SELECT DISTINCT C_PAYMENT_CNT AS distinct_payment_count
        FROM dl.CUSTOMER
    )
);
