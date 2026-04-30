-- {"operators": "AGGREGATE,FILTER,ORDER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK", "openivm_verified": true}
SELECT S_W_ID, LIST(S_I_ID ORDER BY S_QUANTITY DESC) FILTER (WHERE S_QUANTITY > 50) AS well_stocked FROM STOCK GROUP BY S_W_ID;
