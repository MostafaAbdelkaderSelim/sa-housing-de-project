-- models/intermediate/int_transactions_enriched.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Intermediate layer: enriched transactions with window-function KPIs
-- ─────────────────────────────────────────────────────────────────────────────

{{ config(
    materialized = 'table',
    tags         = ['intermediate', 'housing']
) }}

with base as (
    select * from {{ ref('stg_transactions') }}
),

city_stats as (
    select
        city,
        avg(transaction_value_sar)  as city_avg_value,
        avg(price_per_sqm)          as city_avg_price_sqm,
        avg(demand_score)           as city_avg_demand,
        avg(vacancy_rate)           as city_avg_vacancy
    from base
    group by city
),

district_stats as (
    select
        city,
        district,
        count(*)                     as district_txn_count,
        avg(transaction_value_sar)   as district_avg_value,
        avg(demand_score)            as district_avg_demand
    from base
    group by city, district
),

enriched as (
    select
        b.*,

        -- City benchmarks
        cs.city_avg_value,
        cs.city_avg_price_sqm,
        cs.city_avg_demand,
        cs.city_avg_vacancy,

        -- Value vs city average
        round(
            (b.transaction_value_sar - cs.city_avg_value) / cs.city_avg_value * 100,
        2) as pct_above_city_avg,

        -- District context
        ds.district_txn_count,
        ds.district_avg_value,
        ds.district_avg_demand,

        -- Composite risk score (higher = riskier)
        round(
            (b.interest_rate * 100)
            + (b.vacancy_rate * 50)
            - (b.demand_score * 5)
            + (b.infra_distance_km * 0.1),
        2) as risk_score,

        -- Investment attractiveness score
        round(
            (b.demand_score * 15)
            + ((1 - b.vacancy_rate) * 10)
            - (b.interest_rate * 50)
            + (b.demand_supply_ratio * 5),
        2) as investment_score

    from base b
    left join city_stats     cs on b.city = cs.city
    left join district_stats ds on b.city = ds.city and b.district = ds.district
)

select * from enriched
