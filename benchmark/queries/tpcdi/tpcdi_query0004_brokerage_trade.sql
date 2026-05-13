-- {"operators": "PROJECTION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "brokerage_trade"}
-- Bronze: union trade across all batches, normalize CDC columns
-- Batch1 has no cdc prefix; Batch2/3 have cdc_flag + cdc_dsn

select
    t_id,
    t_dts,
    t_st_id,
    t_tt_id,
    t_is_cash,
    t_s_symb,
    t_qty,
    t_bid_price,
    t_ca_id,
    t_exec_name,
    t_trade_price,
    t_chrg,
    t_comm,
    t_tax
from batch1_trade

union all

select
    t_id,
    t_dts,
    t_st_id,
    t_tt_id,
    t_is_cash,
    t_s_symb,
    t_qty,
    t_bid_price,
    t_ca_id,
    t_exec_name,
    t_trade_price,
    t_chrg,
    t_comm,
    t_tax
from batch2_trade

union all

select
    t_id,
    t_dts,
    t_st_id,
    t_tt_id,
    t_is_cash,
    t_s_symb,
    t_qty,
    t_bid_price,
    t_ca_id,
    t_exec_name,
    t_trade_price,
    t_chrg,
    t_comm,
    t_tax
from batch3_trade
