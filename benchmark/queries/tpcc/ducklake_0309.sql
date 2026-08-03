-- {"complexity": "extreme", "is_incremental": true, "ducklake": true, "openivm_verified": true, "operators": "DUCKLAKE,CTE,AGGREGATE,HAVING,TOP_K,ORDER,LIMIT"}
WITH
parameters AS (
	SELECT
		DATE '2026-06-01' AS start_date,
		DATE '2026-06-30' AS end_date,
		0.0::DOUBLE AS zero_amount,
		10::INTEGER AS low_item_cutoff,
		50::INTEGER AS high_stock_cutoff,
		'GC' AS good_credit,
		'BC' AS bad_credit,
		'open' AS open_status,
		'closed' AS closed_status,
		'late' AS late_status


),
date_spine AS (
		SELECT DATE '2026-06-01' AS report_date
		UNION ALL SELECT DATE '2026-06-02' AS report_date
		UNION ALL SELECT DATE '2026-06-03' AS report_date
		UNION ALL SELECT DATE '2026-06-04' AS report_date
		UNION ALL SELECT DATE '2026-06-05' AS report_date
		UNION ALL SELECT DATE '2026-06-06' AS report_date
		UNION ALL SELECT DATE '2026-06-07' AS report_date
		UNION ALL SELECT DATE '2026-06-08' AS report_date
		UNION ALL SELECT DATE '2026-06-09' AS report_date
		UNION ALL SELECT DATE '2026-06-10' AS report_date

),
warehouse_seed AS (
	SELECT
		w.W_ID AS warehouse_id,
		LOWER(TRIM(w.W_NAME)) AS warehouse_name_norm,
		COALESCE(w.W_STATE, 'NA') AS warehouse_state,
		CAST(w.W_YTD AS DOUBLE) AS warehouse_ytd,
		CAST(w.W_TAX AS DOUBLE) AS warehouse_tax,
		CASE WHEN CAST(w.W_TAX AS DOUBLE) >= 0.07 THEN 'high_tax' WHEN CAST(w.W_TAX AS DOUBLE) >= 0.04 THEN 'mid_tax' ELSE 'low_tax' END AS warehouse_flag,
		(w.W_ID % 4) AS warehouse_bucket
	FROM dl.WAREHOUSE w
	WHERE w.W_ID IS NOT NULL

),
warehouse_layer_01 AS (
	SELECT
		warehouse_id,
		warehouse_name_norm,
		warehouse_state,
		warehouse_tax,
		warehouse_bucket,
		(warehouse_ytd + 1 * 0.01) AS warehouse_ytd,
		CASE WHEN warehouse_ytd >= 1 THEN warehouse_flag ELSE warehouse_flag END AS warehouse_flag,
		(1)::INTEGER AS warehouse_stage
	FROM warehouse_seed

),
warehouse_layer_02 AS (
	SELECT
		warehouse_id,
		warehouse_name_norm,
		warehouse_state,
		warehouse_tax,
		warehouse_bucket,
		(warehouse_ytd + 2 * 0.01) AS warehouse_ytd,
		CASE WHEN warehouse_ytd >= 2 THEN warehouse_flag ELSE warehouse_flag END AS warehouse_flag,
		(2)::INTEGER AS warehouse_stage
	FROM warehouse_layer_01

),
warehouse_final AS (
	SELECT
		warehouse_id,
		warehouse_name_norm,
		warehouse_state,
		warehouse_tax,
		warehouse_ytd,
		warehouse_flag,
		warehouse_bucket,
		warehouse_stage
	FROM warehouse_layer_02

),
district_seed AS (
	SELECT
		d.D_W_ID AS warehouse_id,
		d.D_ID AS district_id,
		LOWER(TRIM(d.D_NAME)) AS district_name_norm,
		CAST(d.D_YTD AS DOUBLE) AS district_ytd,
		CAST(d.D_TAX AS DOUBLE) AS district_tax,
		d.D_NEXT_O_ID AS next_order_id,
		CASE WHEN d.D_NEXT_O_ID > 5 THEN 'busy' ELSE 'quiet' END AS district_flag,
		((d.D_W_ID * 10) + d.D_ID) AS district_key
	FROM dl.DISTRICT d
	WHERE d.D_ID BETWEEN 1 AND 10

),
district_layer_01 AS (
	SELECT
		warehouse_id,
		district_id,
		district_name_norm,
		district_tax,
		next_order_id,
		district_key,
		(district_ytd + 1 * 0.01) AS district_ytd,
		CASE WHEN district_ytd >= 1 THEN district_flag ELSE district_flag END AS district_flag,
		(1)::INTEGER AS district_stage
	FROM district_seed

),
district_layer_02 AS (
	SELECT
		warehouse_id,
		district_id,
		district_name_norm,
		district_tax,
		next_order_id,
		district_key,
		(district_ytd + 2 * 0.01) AS district_ytd,
		CASE WHEN district_ytd >= 2 THEN district_flag ELSE district_flag END AS district_flag,
		(2)::INTEGER AS district_stage
	FROM district_layer_01

),
district_final AS (
	SELECT
		warehouse_id,
		district_id,
		district_name_norm,
		district_tax,
		district_ytd,
		next_order_id,
		district_flag,
		district_key,
		district_stage
	FROM district_layer_02

),
customer_seed AS (
	SELECT
		c.C_W_ID AS warehouse_id,
		c.C_D_ID AS district_id,
		c.C_ID AS customer_id,
		LOWER(TRIM(c.C_LAST)) AS customer_last_norm,
		LOWER(TRIM(c.C_FIRST)) AS customer_first_norm,
		COALESCE(NULLIF(TRIM(c.C_CREDIT), ''), 'GC') AS credit_code,
		CAST(c.C_BALANCE AS DOUBLE) AS customer_balance,
		CAST(c.C_YTD_PAYMENT AS DOUBLE) AS customer_ytd_payment,
		CAST(c.C_PAYMENT_CNT AS BIGINT) AS customer_payment_count,
		CAST(c.C_DELIVERY_CNT AS BIGINT) AS customer_delivery_count,
		CAST(c.C_DISCOUNT AS DOUBLE) AS customer_discount,
		CASE WHEN CAST(c.C_BALANCE AS DOUBLE) < 0 THEN 'debt' WHEN CAST(c.C_BALANCE AS DOUBLE) > 20000 THEN 'surplus' ELSE 'steady' END AS customer_flag,
		((c.C_W_ID * 100000) + (c.C_D_ID * 1000) + c.C_ID) AS customer_key
	FROM dl.CUSTOMER c
	WHERE c.C_ID > 0

),
customer_layer_01 AS (
	SELECT
		warehouse_id,
		district_id,
		customer_id,
		customer_last_norm,
		customer_first_norm,
		credit_code,
		customer_ytd_payment,
		customer_payment_count,
		customer_delivery_count,
		customer_discount,
		customer_key,
		(customer_balance + 1 * 0.01) AS customer_balance,
		CASE WHEN customer_balance >= 1 THEN customer_flag ELSE customer_flag END AS customer_flag,
		(1)::INTEGER AS customer_stage
	FROM customer_seed

),
customer_layer_02 AS (
	SELECT
		warehouse_id,
		district_id,
		customer_id,
		customer_last_norm,
		customer_first_norm,
		credit_code,
		customer_ytd_payment,
		customer_payment_count,
		customer_delivery_count,
		customer_discount,
		customer_key,
		(customer_balance + 2 * 0.01) AS customer_balance,
		CASE WHEN customer_balance >= 2 THEN customer_flag ELSE customer_flag END AS customer_flag,
		(2)::INTEGER AS customer_stage
	FROM customer_layer_01

),
customer_layer_03 AS (
	SELECT
		warehouse_id,
		district_id,
		customer_id,
		customer_last_norm,
		customer_first_norm,
		credit_code,
		customer_ytd_payment,
		customer_payment_count,
		customer_delivery_count,
		customer_discount,
		customer_key,
		(customer_balance + 3 * 0.01) AS customer_balance,
		CASE WHEN customer_balance >= 3 THEN customer_flag ELSE customer_flag END AS customer_flag,
		(3)::INTEGER AS customer_stage
	FROM customer_layer_02

),
customer_final AS (
	SELECT
		warehouse_id,
		district_id,
		customer_id,
		customer_last_norm,
		customer_first_norm,
		credit_code,
		customer_balance,
		customer_ytd_payment,
		customer_payment_count,
		customer_delivery_count,
		customer_discount,
		customer_flag,
		customer_key,
		customer_stage
	FROM customer_layer_03

),
order_seed AS (
	SELECT
		o.O_W_ID AS warehouse_id,
		o.O_D_ID AS district_id,
		o.O_ID AS order_id,
		o.O_C_ID AS customer_id,
		COALESCE(o.O_CARRIER_ID, -1) AS carrier_id,
		CAST(o.O_OL_CNT AS BIGINT) AS order_line_count,
		CAST(o.O_ALL_LOCAL AS BIGINT) AS all_local_flag,
		CAST(o.O_ENTRY_D AS DATE) AS order_entry_date,
		CASE WHEN o.O_CARRIER_ID IS NULL THEN 'open' ELSE 'closed' END AS order_flag,
		((o.O_W_ID * 100000) + (o.O_D_ID * 1000) + o.O_ID) AS order_key
	FROM dl.OORDER o
	WHERE o.O_ID IS NOT NULL

),
order_layer_01 AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		customer_id,
		carrier_id,
		all_local_flag,
		order_entry_date,
		order_key,
		(order_line_count + 1 * 0.01) AS order_line_count,
		CASE WHEN order_line_count >= 1 THEN order_flag ELSE order_flag END AS order_flag,
		(1)::INTEGER AS order_stage
	FROM order_seed

),
order_layer_02 AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		customer_id,
		carrier_id,
		all_local_flag,
		order_entry_date,
		order_key,
		(order_line_count + 2 * 0.01) AS order_line_count,
		CASE WHEN order_line_count >= 2 THEN order_flag ELSE order_flag END AS order_flag,
		(2)::INTEGER AS order_stage
	FROM order_layer_01

),
order_final AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		customer_id,
		carrier_id,
		order_line_count,
		all_local_flag,
		order_entry_date,
		order_flag,
		order_key,
		order_stage
	FROM order_layer_02

),
order_line_seed AS (
	SELECT
		ol.OL_W_ID AS warehouse_id,
		ol.OL_D_ID AS district_id,
		ol.OL_O_ID AS order_id,
		ol.OL_NUMBER AS line_number,
		ol.OL_I_ID AS item_id,
		CAST(ol.OL_AMOUNT AS DOUBLE) AS line_amount,
		CAST(ol.OL_QUANTITY AS DOUBLE) AS line_quantity,
		ol.OL_SUPPLY_W_ID AS supply_warehouse_id,
		CAST(ol.OL_DELIVERY_D AS DATE) AS delivery_date,
		CASE WHEN ol.OL_DELIVERY_D IS NULL THEN 'undelivered' ELSE 'delivered' END AS line_flag,
		((ol.OL_W_ID * 1000000) + (ol.OL_D_ID * 10000) + (ol.OL_O_ID * 100) + ol.OL_NUMBER) AS line_key
	FROM dl.ORDER_LINE ol
	WHERE ol.OL_NUMBER > 0

),
order_line_layer_01 AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		line_number,
		item_id,
		line_quantity,
		supply_warehouse_id,
		delivery_date,
		line_key,
		(line_amount + 1 * 0.01) AS line_amount,
		CASE WHEN line_amount >= 1 THEN line_flag ELSE line_flag END AS line_flag,
		(1)::INTEGER AS order_line_stage
	FROM order_line_seed

),
order_line_layer_02 AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		line_number,
		item_id,
		line_quantity,
		supply_warehouse_id,
		delivery_date,
		line_key,
		(line_amount + 2 * 0.01) AS line_amount,
		CASE WHEN line_amount >= 2 THEN line_flag ELSE line_flag END AS line_flag,
		(2)::INTEGER AS order_line_stage
	FROM order_line_layer_01

),
order_line_final AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		line_number,
		item_id,
		line_amount,
		line_quantity,
		supply_warehouse_id,
		delivery_date,
		line_flag,
		line_key,
		order_line_stage
	FROM order_line_layer_02

),
item_seed AS (
	SELECT
		i.I_ID AS item_id,
		LOWER(TRIM(i.I_NAME)) AS item_name_norm,
		CAST(i.I_PRICE AS DOUBLE) AS item_price,
		i.I_IM_ID AS image_id,
		CASE WHEN CAST(i.I_PRICE AS DOUBLE) >= 50 THEN 'premium' ELSE 'standard' END AS item_flag,
		(i.I_ID % 10) AS item_bucket
	FROM dl.ITEM i
	WHERE i.I_ID > 0

),
item_layer_01 AS (
	SELECT
		item_id,
		item_name_norm,
		image_id,
		item_bucket,
		(item_price + 1 * 0.01) AS item_price,
		CASE WHEN item_price >= 1 THEN item_flag ELSE item_flag END AS item_flag,
		(1)::INTEGER AS item_stage
	FROM item_seed

),
item_layer_02 AS (
	SELECT
		item_id,
		item_name_norm,
		image_id,
		item_bucket,
		(item_price + 2 * 0.01) AS item_price,
		CASE WHEN item_price >= 2 THEN item_flag ELSE item_flag END AS item_flag,
		(2)::INTEGER AS item_stage
	FROM item_layer_01

),
item_final AS (
	SELECT
		item_id,
		item_name_norm,
		item_price,
		image_id,
		item_flag,
		item_bucket,
		item_stage
	FROM item_layer_02

),
stock_seed AS (
	SELECT
		s.S_W_ID AS warehouse_id,
		s.S_I_ID AS item_id,
		CAST(s.S_QUANTITY AS BIGINT) AS stock_quantity,
		CAST(s.S_YTD AS DOUBLE) AS stock_ytd,
		CAST(s.S_ORDER_CNT AS BIGINT) AS stock_order_count,
		CAST(s.S_REMOTE_CNT AS BIGINT) AS stock_remote_count,
		CASE WHEN s.S_QUANTITY >= 75 THEN 'deep' WHEN s.S_QUANTITY >= 50 THEN 'normal' ELSE 'thin' END AS stock_flag,
		((s.S_W_ID * 1000) + s.S_I_ID) AS stock_key
	FROM dl.STOCK s
	WHERE s.S_I_ID > 0

),
stock_layer_01 AS (
	SELECT
		warehouse_id,
		item_id,
		stock_ytd,
		stock_order_count,
		stock_remote_count,
		stock_key,
		(stock_quantity + 1 * 0.01) AS stock_quantity,
		CASE WHEN stock_quantity >= 1 THEN stock_flag ELSE stock_flag END AS stock_flag,
		(1)::INTEGER AS stock_stage
	FROM stock_seed

),
stock_layer_02 AS (
	SELECT
		warehouse_id,
		item_id,
		stock_ytd,
		stock_order_count,
		stock_remote_count,
		stock_key,
		(stock_quantity + 2 * 0.01) AS stock_quantity,
		CASE WHEN stock_quantity >= 2 THEN stock_flag ELSE stock_flag END AS stock_flag,
		(2)::INTEGER AS stock_stage
	FROM stock_layer_01

),
stock_final AS (
	SELECT
		warehouse_id,
		item_id,
		stock_quantity,
		stock_ytd,
		stock_order_count,
		stock_remote_count,
		stock_flag,
		stock_key,
		stock_stage
	FROM stock_layer_02

),
new_order_seed AS (
	SELECT
		no.NO_W_ID AS warehouse_id,
		no.NO_D_ID AS district_id,
		no.NO_O_ID AS order_id,
		'new' AS new_order_flag,
		((no.NO_W_ID * 100000) + (no.NO_D_ID * 1000) + no.NO_O_ID) AS new_order_key
	FROM dl.NEW_ORDER no

),
new_order_layer_01 AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		(new_order_key + 1 * 0.01) AS new_order_key,
		CASE WHEN new_order_key >= 1 THEN new_order_flag ELSE new_order_flag END AS new_order_flag,
		(1)::INTEGER AS new_order_stage
	FROM new_order_seed

),
new_order_final AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		new_order_flag,
		new_order_key,
		new_order_stage
	FROM new_order_layer_01

),
history_seed AS (
	SELECT
		h.H_C_W_ID AS warehouse_id,
		h.H_C_D_ID AS district_id,
		h.H_C_ID AS customer_id,
		h.H_W_ID AS history_warehouse_id,
		h.H_D_ID AS history_district_id,
		CAST(h.H_DATE AS DATE) AS history_date,
		CAST(h.H_AMOUNT AS DOUBLE) AS history_amount,
		LOWER(COALESCE(h.H_DATA, '')) AS history_data_norm,
		CASE WHEN LOWER(COALESCE(h.H_DATA, '')) LIKE '%payment%' THEN 'payment' ELSE 'other' END AS history_flag,
		((h.H_C_W_ID * 100000) + (h.H_C_D_ID * 1000) + h.H_C_ID) AS history_customer_key
	FROM dl.HISTORY h
	WHERE h.H_C_ID IS NOT NULL

),
history_layer_01 AS (
	SELECT
		warehouse_id,
		district_id,
		customer_id,
		history_warehouse_id,
		history_district_id,
		history_date,
		history_data_norm,
		history_customer_key,
		(history_amount + 1 * 0.01) AS history_amount,
		CASE WHEN history_amount >= 1 THEN history_flag ELSE history_flag END AS history_flag,
		(1)::INTEGER AS history_stage
	FROM history_seed

),
history_layer_02 AS (
	SELECT
		warehouse_id,
		district_id,
		customer_id,
		history_warehouse_id,
		history_district_id,
		history_date,
		history_data_norm,
		history_customer_key,
		(history_amount + 2 * 0.01) AS history_amount,
		CASE WHEN history_amount >= 2 THEN history_flag ELSE history_flag END AS history_flag,
		(2)::INTEGER AS history_stage
	FROM history_layer_01

),
history_final AS (
	SELECT
		warehouse_id,
		district_id,
		customer_id,
		history_warehouse_id,
		history_district_id,
		history_date,
		history_amount,
		history_data_norm,
		history_flag,
		history_customer_key,
		history_stage
	FROM history_layer_02

),
customer_district_joined AS (
	SELECT
		cf.warehouse_id,
		cf.district_id,
		cf.customer_id,
		cf.customer_last_norm,
		cf.customer_first_norm,
		cf.credit_code,
		cf.customer_balance,
		cf.customer_ytd_payment,
		cf.customer_payment_count,
		cf.customer_delivery_count,
		cf.customer_discount,
		cf.customer_flag,
		cf.customer_key,
		df.district_name_norm,
		df.district_tax,
		df.district_ytd,
		df.next_order_id,
		df.district_flag,
		wf.warehouse_name_norm,
		wf.warehouse_state,
		wf.warehouse_tax,
		wf.warehouse_ytd,
		wf.warehouse_flag,
		CASE WHEN cf.customer_balance < 0 THEN 'watch' WHEN cf.customer_payment_count > 5 THEN 'active' ELSE 'base' END AS customer_segment
	FROM customer_final cf
	JOIN district_final df
		ON df.warehouse_id = cf.warehouse_id
		AND df.district_id = cf.district_id
	JOIN warehouse_final wf
		ON wf.warehouse_id = cf.warehouse_id

),
order_customer_joined AS (
	SELECT
		ofn.warehouse_id,
		ofn.district_id,
		ofn.order_id,
		ofn.customer_id,
		ofn.carrier_id,
		ofn.order_line_count,
		ofn.all_local_flag,
		ofn.order_entry_date,
		ofn.order_flag,
		ofn.order_key,
		cdj.customer_last_norm,
		cdj.customer_first_norm,
		cdj.credit_code,
		cdj.customer_balance,
		cdj.customer_ytd_payment,
		cdj.customer_payment_count,
		cdj.customer_delivery_count,
		cdj.customer_discount,
		cdj.customer_flag,
		cdj.customer_segment,
		cdj.district_name_norm,
		cdj.district_tax,
		cdj.district_ytd,
		cdj.next_order_id,
		cdj.district_flag,
		cdj.warehouse_name_norm,
		cdj.warehouse_state,
		cdj.warehouse_tax,
		cdj.warehouse_ytd,
		cdj.warehouse_flag
	FROM order_final ofn
	JOIN customer_district_joined cdj
		ON cdj.warehouse_id = ofn.warehouse_id
		AND cdj.district_id = ofn.district_id
		AND cdj.customer_id = ofn.customer_id

),
line_fact_seed AS (
	SELECT
		ocj.warehouse_id,
		ocj.district_id,
		ocj.order_id,
		ocj.customer_id,
		olf.line_number,
		olf.item_id,
		olf.line_amount,
		olf.line_quantity,
		olf.supply_warehouse_id,
		olf.delivery_date,
		olf.line_flag,
		olf.line_key,
		ocj.carrier_id,
		ocj.order_line_count,
		ocj.all_local_flag,
		ocj.order_entry_date,
		ocj.order_flag,
		ocj.customer_last_norm,
		ocj.customer_first_norm,
		ocj.credit_code,
		ocj.customer_balance,
		ocj.customer_ytd_payment,
		ocj.customer_payment_count,
		ocj.customer_delivery_count,
		ocj.customer_discount,
		ocj.customer_flag,
		ocj.customer_segment,
		ocj.district_name_norm,
		ocj.district_tax,
		ocj.district_ytd,
		ocj.next_order_id,
		ocj.district_flag,
		ocj.warehouse_name_norm,
		ocj.warehouse_state,
		ocj.warehouse_tax,
		ocj.warehouse_ytd,
		ocj.warehouse_flag,
		it.item_name_norm,
		it.item_price,
		it.image_id,
		it.item_flag,
		it.item_bucket,
		COALESCE(st.stock_quantity, 0) AS stock_quantity,
		COALESCE(st.stock_ytd, 0.0) AS stock_ytd,
		COALESCE(st.stock_order_count, 0) AS stock_order_count,
		COALESCE(st.stock_remote_count, 0) AS stock_remote_count,
		COALESCE(st.stock_flag, 'missing') AS stock_flag,
		CASE WHEN nof.order_id IS NOT NULL THEN 'new_order' WHEN ocj.carrier_id = -1 THEN 'open' ELSE 'closed' END AS fulfillment_state,
		(olf.line_amount * olf.line_quantity) AS extended_amount
	FROM order_customer_joined ocj
	JOIN order_line_final olf
		ON olf.warehouse_id = ocj.warehouse_id
		AND olf.district_id = ocj.district_id
		AND olf.order_id = ocj.order_id
	JOIN item_final it
		ON it.item_id = olf.item_id
	LEFT JOIN stock_final st
		ON st.warehouse_id = olf.supply_warehouse_id
		AND st.item_id = olf.item_id
	LEFT JOIN new_order_final nof
		ON nof.warehouse_id = ocj.warehouse_id
		AND nof.district_id = ocj.district_id
		AND nof.order_id = ocj.order_id

),
history_customer_rollup AS (
	SELECT
		hf.warehouse_id,
		hf.district_id,
		hf.customer_id,
		COUNT(*) AS history_rows,
		COUNT(*) FILTER (WHERE hf.history_flag = 'payment') AS payment_history_rows,
		SUM(hf.history_amount) AS total_history_amount,
		MIN(hf.history_amount) AS min_history_amount,
		MAX(hf.history_amount) AS max_history_amount,
		MAX(hf.history_date) AS latest_history_date
	FROM history_final hf
	GROUP BY
		hf.warehouse_id,
		hf.district_id,
		hf.customer_id

),
fact_enriched_seed AS (
	SELECT
		lfs.warehouse_id,
		lfs.district_id,
		lfs.order_id,
		lfs.customer_id,
		lfs.line_number,
		lfs.item_id,
		lfs.line_amount,
		lfs.line_quantity,
		lfs.supply_warehouse_id,
		lfs.delivery_date,
		lfs.line_flag,
		lfs.line_key,
		lfs.carrier_id,
		lfs.order_line_count,
		lfs.all_local_flag,
		lfs.order_entry_date,
		lfs.order_flag,
		lfs.customer_last_norm,
		lfs.customer_first_norm,
		lfs.credit_code,
		lfs.customer_balance,
		lfs.customer_ytd_payment,
		lfs.customer_payment_count,
		lfs.customer_delivery_count,
		lfs.customer_discount,
		lfs.customer_flag,
		lfs.customer_segment,
		lfs.district_name_norm,
		lfs.district_tax,
		lfs.district_ytd,
		lfs.next_order_id,
		lfs.district_flag,
		lfs.warehouse_name_norm,
		lfs.warehouse_state,
		lfs.warehouse_tax,
		lfs.warehouse_ytd,
		lfs.warehouse_flag,
		lfs.item_name_norm,
		lfs.item_price,
		lfs.image_id,
		lfs.item_flag,
		lfs.item_bucket,
		lfs.stock_quantity,
		lfs.stock_ytd,
		lfs.stock_order_count,
		lfs.stock_remote_count,
		lfs.stock_flag,
		lfs.fulfillment_state,
		lfs.extended_amount,
		COALESCE(hcr.history_rows, 0) AS history_rows,
		COALESCE(hcr.payment_history_rows, 0) AS payment_history_rows,
		COALESCE(hcr.total_history_amount, 0.0) AS total_history_amount,
		COALESCE(hcr.min_history_amount, 0.0) AS min_history_amount,
		COALESCE(hcr.max_history_amount, 0.0) AS max_history_amount,
		hcr.latest_history_date,
		CASE WHEN COALESCE(hcr.payment_history_rows, 0) > 0 THEN 'paid' WHEN lfs.customer_balance < 0 THEN 'debt' ELSE 'neutral' END AS account_state
	FROM line_fact_seed lfs
	LEFT JOIN history_customer_rollup hcr
		ON hcr.warehouse_id = lfs.warehouse_id
		AND hcr.district_id = lfs.district_id
		AND hcr.customer_id = lfs.customer_id

),
fact_stage_01 AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		customer_id,
		line_number,
		item_id,
		line_amount,
		line_quantity,
		supply_warehouse_id,
		delivery_date,
		line_flag,
		line_key,
		carrier_id,
		order_line_count,
		all_local_flag,
		order_entry_date,
		order_flag,
		customer_last_norm,
		customer_first_norm,
		credit_code,
		customer_balance,
		customer_ytd_payment,
		customer_payment_count,
		customer_delivery_count,
		customer_discount,
		customer_flag,
		customer_segment,
		district_name_norm,
		district_tax,
		district_ytd,
		next_order_id,
		district_flag,
		warehouse_name_norm,
		warehouse_state,
		warehouse_tax,
		warehouse_ytd,
		warehouse_flag,
		item_name_norm,
		item_price,
		image_id,
		item_flag,
		item_bucket,
		stock_quantity,
		stock_ytd,
		stock_order_count,
		stock_remote_count,
		stock_flag,
		fulfillment_state,
		(extended_amount + 1 * 0.001) AS extended_amount,
		history_rows,
		payment_history_rows,
		total_history_amount,
		min_history_amount,
		max_history_amount,
		latest_history_date,
		account_state,
		CASE WHEN extended_amount > 5 THEN 'amount_hi' ELSE account_state END AS fact_flag_01
	FROM fact_enriched_seed

),
fact_stage_02 AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		customer_id,
		line_number,
		item_id,
		line_amount,
		line_quantity,
		supply_warehouse_id,
		delivery_date,
		line_flag,
		line_key,
		carrier_id,
		order_line_count,
		all_local_flag,
		order_entry_date,
		order_flag,
		customer_last_norm,
		customer_first_norm,
		credit_code,
		customer_balance,
		customer_ytd_payment,
		customer_payment_count,
		customer_delivery_count,
		customer_discount,
		customer_flag,
		customer_segment,
		district_name_norm,
		district_tax,
		district_ytd,
		next_order_id,
		district_flag,
		warehouse_name_norm,
		warehouse_state,
		warehouse_tax,
		warehouse_ytd,
		warehouse_flag,
		item_name_norm,
		item_price,
		image_id,
		item_flag,
		item_bucket,
		stock_quantity,
		stock_ytd,
		stock_order_count,
		stock_remote_count,
		stock_flag,
		fulfillment_state,
		(extended_amount + 2 * 0.001) AS extended_amount,
		history_rows,
		payment_history_rows,
		total_history_amount,
		min_history_amount,
		max_history_amount,
		latest_history_date,
		account_state,
		CASE WHEN extended_amount > 10 THEN 'amount_hi' ELSE account_state END AS fact_flag_02
	FROM fact_stage_01

),
fact_stage_03 AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		customer_id,
		line_number,
		item_id,
		line_amount,
		line_quantity,
		supply_warehouse_id,
		delivery_date,
		line_flag,
		line_key,
		carrier_id,
		order_line_count,
		all_local_flag,
		order_entry_date,
		order_flag,
		customer_last_norm,
		customer_first_norm,
		credit_code,
		customer_balance,
		customer_ytd_payment,
		customer_payment_count,
		customer_delivery_count,
		customer_discount,
		customer_flag,
		customer_segment,
		district_name_norm,
		district_tax,
		district_ytd,
		next_order_id,
		district_flag,
		warehouse_name_norm,
		warehouse_state,
		warehouse_tax,
		warehouse_ytd,
		warehouse_flag,
		item_name_norm,
		item_price,
		image_id,
		item_flag,
		item_bucket,
		stock_quantity,
		stock_ytd,
		stock_order_count,
		stock_remote_count,
		stock_flag,
		fulfillment_state,
		(extended_amount + 3 * 0.001) AS extended_amount,
		history_rows,
		payment_history_rows,
		total_history_amount,
		min_history_amount,
		max_history_amount,
		latest_history_date,
		account_state,
		CASE WHEN extended_amount > 15 THEN 'amount_hi' ELSE account_state END AS fact_flag_03
	FROM fact_stage_02

),
fact_stage_04 AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		customer_id,
		line_number,
		item_id,
		line_amount,
		line_quantity,
		supply_warehouse_id,
		delivery_date,
		line_flag,
		line_key,
		carrier_id,
		order_line_count,
		all_local_flag,
		order_entry_date,
		order_flag,
		customer_last_norm,
		customer_first_norm,
		credit_code,
		customer_balance,
		customer_ytd_payment,
		customer_payment_count,
		customer_delivery_count,
		customer_discount,
		customer_flag,
		customer_segment,
		district_name_norm,
		district_tax,
		district_ytd,
		next_order_id,
		district_flag,
		warehouse_name_norm,
		warehouse_state,
		warehouse_tax,
		warehouse_ytd,
		warehouse_flag,
		item_name_norm,
		item_price,
		image_id,
		item_flag,
		item_bucket,
		stock_quantity,
		stock_ytd,
		stock_order_count,
		stock_remote_count,
		stock_flag,
		fulfillment_state,
		(extended_amount + 4 * 0.001) AS extended_amount,
		history_rows,
		payment_history_rows,
		total_history_amount,
		min_history_amount,
		max_history_amount,
		latest_history_date,
		account_state,
		CASE WHEN extended_amount > 20 THEN 'amount_hi' ELSE account_state END AS fact_flag_04
	FROM fact_stage_03

),
fact_final AS (
	SELECT
		warehouse_id,
		district_id,
		order_id,
		customer_id,
		line_number,
		item_id,
		line_amount,
		line_quantity,
		supply_warehouse_id,
		delivery_date,
		line_flag,
		line_key,
		carrier_id,
		order_line_count,
		all_local_flag,
		order_entry_date,
		order_flag,
		customer_last_norm,
		customer_first_norm,
		credit_code,
		customer_balance,
		customer_ytd_payment,
		customer_payment_count,
		customer_delivery_count,
		customer_discount,
		customer_flag,
		customer_segment,
		district_name_norm,
		district_tax,
		district_ytd,
		next_order_id,
		district_flag,
		warehouse_name_norm,
		warehouse_state,
		warehouse_tax,
		warehouse_ytd,
		warehouse_flag,
		item_name_norm,
		item_price,
		image_id,
		item_flag,
		item_bucket,
		stock_quantity,
		stock_ytd,
		stock_order_count,
		stock_remote_count,
		stock_flag,
		fulfillment_state,
		extended_amount,
		history_rows,
		payment_history_rows,
		total_history_amount,
		min_history_amount,
		max_history_amount,
		latest_history_date,
		account_state
	FROM fact_stage_04

),
topk_rollup AS (
	SELECT
		ff.warehouse_id,
		ff.district_id,
		ff.customer_id,
		ff.credit_code,
		COUNT(*) AS line_rows,
		SUM(ff.extended_amount) AS total_amount,
		SUM(ff.stock_quantity) AS stock_quantity_sum,
		MIN(ff.customer_balance) AS min_balance,
		MAX(ff.customer_balance) AS max_balance
	FROM fact_stage_01 ff
	GROUP BY
		ff.warehouse_id,
		ff.district_id,
		ff.customer_id,
		ff.credit_code
	HAVING COUNT(*) > 0

),
topk_projected AS (
	SELECT
		warehouse_id,
		district_id,
		customer_id,
		credit_code,
		line_rows,
		total_amount,
		stock_quantity_sum,
		min_balance,
		max_balance
	FROM topk_rollup

)
SELECT
	warehouse_id,
	district_id,
	customer_id,
	credit_code,
	line_rows,
	total_amount,
	stock_quantity_sum,
	min_balance,
	max_balance
FROM topk_projected
ORDER BY ROUND(total_amount, 10) DESC, warehouse_id ASC, district_id ASC, customer_id ASC
LIMIT 25 OFFSET 2
