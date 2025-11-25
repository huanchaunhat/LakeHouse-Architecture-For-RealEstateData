{{ config(
    materialized='table',
    file_format='delta'
) }}

-- Fact table: Daily Summary (Aggregated Fact)
-- Pre-aggregated metrics for performance

select
    -- Time dimension
    f.date_key as report_date,
    
    -- AGGREGATIONS: Proper NULL handling
    -- Count always returns a number (never NULL)
    count(distinct f.property_id) as total_new_listings,
    
    -- Sum/Avg of guaranteed NOT NULL fields (from WHERE filters in fct_properties)
    round(sum(f.price_in_billions), 2) as total_value_listed_billions,
    round(avg(f.price_per_m2_millions), 2) as avg_price_per_m2_millions,
    round(min(f.price_per_m2_millions), 2) as min_price_per_m2_millions,
    round(max(f.price_per_m2_millions), 2) as max_price_per_m2_millions,
    round(avg(f.area), 2) as avg_area,
    
    -- Optional fields: Ignore NULLs in AVG (correct behavior)
    -- AVG() automatically ignores NULL values
    round(avg(cast(f.bedrooms as double)), 1) as avg_bedrooms,
    round(avg(cast(f.bathrooms as double)), 1) as avg_bathrooms,
    round(avg(cast(f.floors as double)), 1) as avg_floors,
    
    -- Count non-NULL optional fields to show data completeness
    count(f.bedrooms) as properties_with_bedroom_info,
    count(f.bathrooms) as properties_with_bathroom_info,
    count(f.floors) as properties_with_floor_info
    
from {{ ref('fct_properties') }} f

-- No WHERE filter needed - all records are valid
group by f.date_key
order by f.date_key desc