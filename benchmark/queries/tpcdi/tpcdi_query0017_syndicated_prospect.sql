-- {"operators": "PROJECTION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "syndicated_prospect"}
-- Bronze: union prospect across all batches
select
    agencyid as agency_id,
    lastname as last_name,
    firstname as first_name,
    middleinitial as middle_initial,
    gender,
    addressline1 as address_line1,
    addressline2 as address_line2,
    postalcode as postal_code,
    city,
    state,
    country,
    phone,
    income,
    numbercars as number_cars,
    numberchildren as number_children,
    maritalstatus as marital_status,
    age,
    creditrating as credit_rating,
    ownorrentflag as own_or_rent_flag,
    employer,
    numbercreditcards as number_credit_cards,
    networth as net_worth
from batch1_prospect

union all

select
    agencyid as agency_id,
    lastname as last_name,
    firstname as first_name,
    middleinitial as middle_initial,
    gender,
    addressline1 as address_line1,
    addressline2 as address_line2,
    postalcode as postal_code,
    city,
    state,
    country,
    phone,
    income,
    numbercars as number_cars,
    numberchildren as number_children,
    maritalstatus as marital_status,
    age,
    creditrating as credit_rating,
    ownorrentflag as own_or_rent_flag,
    employer,
    numbercreditcards as number_credit_cards,
    networth as net_worth
from batch2_prospect

union all

select
    agencyid as agency_id,
    lastname as last_name,
    firstname as first_name,
    middleinitial as middle_initial,
    gender,
    addressline1 as address_line1,
    addressline2 as address_line2,
    postalcode as postal_code,
    city,
    state,
    country,
    phone,
    income,
    numbercars as number_cars,
    numberchildren as number_children,
    maritalstatus as marital_status,
    age,
    creditrating as credit_rating,
    ownorrentflag as own_or_rent_flag,
    employer,
    numbercreditcards as number_credit_cards,
    networth as net_worth
from batch3_prospect
