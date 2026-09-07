-- {"operators": "LIMIT,DISTINCT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "mv_verified": true, "refresh_type": "FULL_REFRESH"}
SELECT DISTINCT OL_W_ID FROM ORDER_LINE LIMIT 10;
