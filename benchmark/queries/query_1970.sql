-- {"operators": "AGGREGATE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "ITEM", "openivm_verified": true}
SELECT I_IM_ID, ROUND(AVG(I_PRICE::DOUBLE), 4) AS avg_price, ROUND(STDDEV(I_PRICE::DOUBLE), 4) AS std_price FROM ITEM GROUP BY I_IM_ID;
