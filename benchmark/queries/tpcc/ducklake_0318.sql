-- {"operators": "DUCKLAKE,WINDOW", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "ducklake": true, "openivm_verified": true}
SELECT
    OL_W_ID AS warehouse_id,
    OL_D_ID AS district_id,
    OL_O_ID AS order_id,
    OL_NUMBER AS line_number,
    OL_DELIVERY_D AS delivery_at,
    ROW_NUMBER() OVER (
        PARTITION BY OL_DELIVERY_D
        ORDER BY OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER
    ) AS delivery_row_number
FROM dl.ORDER_LINE;
