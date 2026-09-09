-- {"operators": "LIMIT,DISTINCT", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ORDER_LINE", "refresh_type": "FULL_REFRESH", "mv_verified": true}
SELECT DISTINCT OL_W_ID FROM ORDER_LINE LIMIT 10;
