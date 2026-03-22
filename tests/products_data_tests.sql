select *
from {{ ref('stg_products') }}
where unit_cost <= 0