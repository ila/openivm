-- {"operators": "UNION_ALL", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "WAREHOUSE,DISTRICT", "openivm_verified": true}
SELECT W_ID AS entity_id, W_NAME AS entity_name, 'warehouse' AS entity_type FROM WAREHOUSE UNION ALL SELECT D_W_ID * 100 + D_ID AS entity_id, D_NAME AS entity_name, 'district' AS entity_type FROM DISTRICT;
