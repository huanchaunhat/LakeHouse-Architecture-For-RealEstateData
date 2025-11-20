select
    date_trunc('day', updated_at_ts) as report_date,
    count(property_id) as total_new_listings,
    sum(price_in_billions) as total_value_listed_billions,
    avg(price_per_m2_millions) as avg_price_per_m2_millions
    
from {{ ref('fct_properties') }} -- Đọc từ bảng fact

group by 1
order by 1 desc