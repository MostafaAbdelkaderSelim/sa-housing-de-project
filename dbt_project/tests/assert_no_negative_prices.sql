-- tests/assert_no_negative_prices.sql
-- Fails if any price_per_sqm is negative

select *
from {{ ref('stg_transactions') }}
where price_per_sqm < 0
