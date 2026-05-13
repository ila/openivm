-- {"operators": "OUTER_JOIN,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "dim_customer"}
-- Gold: dim_customer with forward-fill and prospect enrichment
with s1 as (
    select c.*,
           p.agency_id,
           p.credit_rating,
           p.net_worth
    from customers c
    left join syndicated_prospect p
        on c.first_name = p.first_name
        and c.last_name = p.last_name
        and c.postal_code = p.postal_code
        and c.address_line1 = p.address_line1
        and coalesce(c.address_line2, '') = coalesce(p.address_line2, '')
),
s2 as (
    select
        md5(customer_id || '|' || effective_timestamp) as sk_customer_id,
        customer_id,
        coalesce(tax_id, last_value(tax_id IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as tax_id,
        status,
        coalesce(last_name, last_value(last_name IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as last_name,
        coalesce(first_name, last_value(first_name IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as first_name,
        coalesce(middle_name, last_value(middle_name IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as middleinitial,
        coalesce(gender, last_value(gender IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as gender,
        coalesce(tier, last_value(tier IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as tier,
        coalesce(dob, last_value(dob IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as dob,
        coalesce(address_line1, last_value(address_line1 IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as address_line1,
        coalesce(address_line2, last_value(address_line2 IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as address_line2,
        coalesce(postal_code, last_value(postal_code IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as postal_code,
        coalesce(city, last_value(city IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as city,
        coalesce(state_province, last_value(state_province IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as state_province,
        coalesce(country, last_value(country IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as country,
        coalesce(phone1, last_value(phone1 IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as phone1,
        coalesce(phone2, last_value(phone2 IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as phone2,
        coalesce(phone3, last_value(phone3 IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as phone3,
        coalesce(primary_email, last_value(primary_email IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as primary_email,
        coalesce(alternate_email, last_value(alternate_email IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as alternate_email,
        coalesce(local_tax_rate_name, last_value(local_tax_rate_name IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as local_tax_rate_name,
        coalesce(local_tax_rate, last_value(local_tax_rate IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as local_tax_rate,
        coalesce(national_tax_rate_name, last_value(national_tax_rate_name IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as national_tax_rate_name,
        coalesce(national_tax_rate, last_value(national_tax_rate IGNORE NULLS) over (
            partition by customer_id order by effective_timestamp)) as national_tax_rate,
        agency_id,
        credit_rating,
        net_worth,
        effective_timestamp,
        end_timestamp,
        is_current
    from s1
)
select *
from s2
