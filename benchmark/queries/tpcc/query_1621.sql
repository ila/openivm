-- {"operators": "INNER_JOIN,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,STOCK"}
SELECT DISTINCT i.I_IM_ID, s.S_W_ID FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID;
