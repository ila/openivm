-- {"operators": "PROJECTION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "brokerage_cash_transaction"}
-- Bronze: union cash_transaction across all batches
select
    ct_ca_id,
    ct_dts,
    ct_amt,
    ct_name
from batch1_cash_transaction

union all

select
    ct_ca_id,
    ct_dts,
    ct_amt,
    ct_name
from batch2_cash_transaction

union all

select
    ct_ca_id,
    ct_dts,
    ct_amt,
    ct_name
from batch3_cash_transaction
