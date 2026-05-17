-- models/marts/mart_city_performance.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Mart: City-level KPI dashboard table
-- ─────────────────────────────────────────────────────────────────────────────

{{ config(
    materialized = 'table',
    tags         = ['mart', 'housing', 'dashboard']
) }}

with enriched as (
    select * from {{ ref('int_transactions_enriched') }}
),

city_perf as (
    select
        city,
        txn_year                                        as year,

        -- Volume
        count(*)                                        as total_transactions,
        count(distinct district)                        as active_districts,
        sum(transaction_value_sar)                      as total_market_value_sar,

        -- Pricing
        round(avg(transaction_value_sar), 0)            as avg_transaction_value,
        round(avg(price_per_sqm), 2)                    as avg_price_per_sqm,
        round(min(price_per_sqm), 2)                    as min_price_per_sqm,
        round(max(price_per_sqm), 2)                    as max_price_per_sqm,
        round(avg(area_sqm), 1)                         as avg_area_sqm,

        -- Market health
        round(avg(demand_score), 3)                     as avg_demand_score,
        round(avg(supply_score), 3)                     as avg_supply_score,
        round(avg(demand_supply_ratio), 3)              as avg_demand_supply_ratio,
        round(avg(vacancy_rate) * 100, 2)               as avg_vacancy_pct,
        round(avg(interest_rate) * 100, 2)              as avg_interest_rate_pct,

        -- Risk & Investment
        round(avg(risk_score), 2)                       as avg_risk_score,
        round(avg(investment_score), 2)                 as avg_investment_score,

        -- High demand share
        round(
            sum(case when demand_score > 7 then 1 else 0 end) * 100.0 / count(*),
        2)                                              as high_demand_share_pct

    from enriched
    group by city, txn_year
)

select
    *,
    rank() over (partition by year order by avg_investment_score desc) as investment_rank,
    rank() over (partition by year order by total_transactions desc)   as volume_rank
from city_perf
order by year, total_transactions desc
