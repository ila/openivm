-- {"operators": "OUTER_JOIN,WINDOW,CASE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "source": "ivm-bench/duckdb", "tpcdi": "customers"}
-- Silver: customers with SCD Type 2
select
    action_type,
    case action_type
        when 'NEW' then 'Active'
        when 'ADDACCT' then 'Active'
        when 'UPDACCT' then 'Active'
        when 'UPDCUST' then 'Active'
        when 'INACT' then 'Inactive'
    end as status,
    c_id as customer_id,
    ca_id as account_id,
    c_tax_id as tax_id,
    c_gndr as gender,
    c_tier as tier,
    c_dob as dob,
    c_l_name as last_name,
    c_f_name as first_name,
    c_m_name as middle_name,
    c_adline1 as address_line1,
    c_adline2 as address_line2,
    c_zipcode as postal_code,
    c_city as city,
    c_state_prov as state_province,
    c_ctry as country,
    c_prim_email as primary_email,
    c_alt_email as alternate_email,
    c_phone_1 as phone1,
    c_phone_2 as phone2,
    c_phone_3 as phone3,
    c_lcl_tx_id as local_tax_rate_name,
    ltx.tx_rate as local_tax_rate,
    c_nat_tx_id as national_tax_rate_name,
    ntx.tx_rate as national_tax_rate,
    ca_tax_st as account_tax_status,
    ca_b_id as broker_id,
    action_ts as effective_timestamp,
    coalesce(
        lag(action_ts) over (
            partition by c_id
            order by action_ts desc
        ) - INTERVAL 1 MILLISECOND,
        TIMESTAMP '9999-12-31 23:59:59.999'
    ) as end_timestamp,
    case
        when row_number() over (
            partition by c_id
            order by action_ts desc
        ) = 1 then true
        else false
    end as is_current
from
    crm_customer_mgmt c
left join
    reference_tax_rate ntx
on
    c.c_nat_tx_id = ntx.tx_id
left join
    reference_tax_rate ltx
on
    c.c_lcl_tx_id = ltx.tx_id
where action_type in ('NEW', 'INACT', 'UPDCUST')
