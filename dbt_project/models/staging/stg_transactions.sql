-- models/staging/stg_transactions.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Staging layer: raw CSV → cleaned, typed, renamed columns
-- One record per transaction. No aggregations here.
-- ─────────────────────────────────────────────────────────────────────────────

{{ config(
    materialized = 'view',
    tags         = ['staging', 'housing']
) }}

with source as (
    select * from {{ source('housing_raw', 'transactions') }}
),

cleaned as (
    select
        -- Keys
        trim(Transaction_ID)                        as transaction_id,

        -- Dates
        cast(Date as date)                          as transaction_date,
        extract(year  from cast(Date as date))      as txn_year,
        extract(month from cast(Date as date))      as txn_month,
        extract(quarter from cast(Date as date))    as txn_quarter,

        -- Dimensions
        initcap(trim(City))                         as city,
        initcap(trim(District))                     as district,
        initcap(trim(Property_Type))                as property_type,
        initcap(trim(Purpose))                      as purpose,

        -- Financials
        cast(Transaction_Value as bigint)           as transaction_value_sar,
        cast(Area_sqm as float)                     as area_sqm,

        -- Computed
        round(
            cast(Transaction_Value as float) /
            nullif(cast(Area_sqm as float), 0),
        2)                                          as price_per_sqm,

        -- Other numeric fields
        cast(Number_of_Units as integer)            as number_of_units,
        cast(Contract_Duration as integer)          as contract_duration_months,
        round(cast(Demand_Score as float), 4)       as demand_score,
        round(cast(Supply_Score as float), 4)       as supply_score,
        round(cast(Interest_Rate as float), 4)      as interest_rate,
        round(cast(Infrastructure_Distance_KM as float), 2) as infra_distance_km,
        cast(Population_Migration_Inflow as integer) as population_inflow,
        round(cast(Vacancy_Rate as float), 4)       as vacancy_rate,

        -- Derived flags
        case
            when cast(Transaction_Value as bigint) < 500000   then '<500K'
            when cast(Transaction_Value as bigint) < 1000000  then '500K-1M'
            when cast(Transaction_Value as bigint) < 2000000  then '1M-2M'
            when cast(Transaction_Value as bigint) < 5000000  then '2M-5M'
            else '>5M'
        end                                         as value_band,

        case
            when cast(Demand_Score as float) > cast(Supply_Score as float)
            then 1 else 0
        end                                         as is_demand_dominant,

        round(
            cast(Demand_Score as float) /
            nullif(cast(Supply_Score as float), 0),
        4)                                          as demand_supply_ratio

    from source
    where
        Transaction_ID     is not null
        and Date           is not null
        and Transaction_Value > 0
        and Area_sqm       > 0
)

select * from cleaned
