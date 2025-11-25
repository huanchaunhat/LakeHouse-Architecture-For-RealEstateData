{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='property_id',
    incremental_strategy='merge'
) }}

-- Fact table: Property Listings (Star Schema)
-- Central fact table with foreign keys to dimensions
-- Incremental: Only process new/updated records

with source_properties as (
    select * from {{ ref('stg_properties') }}
    {% if is_incremental() %}
    where updated_at_ts > (select max(updated_at_ts) from {{ this }})
    {% endif %}
),

properties as (
    select 
        *,
        row_number() over (
            partition by property_id 
            order by updated_at_ts desc, created_at desc
        ) as rn
    from source_properties
    where 
        data_quality_flag = 'VALID'
        
        -- CRITICAL FIELDS: Must have values (no NULLs allowed)
        and property_id is not null
        and title is not null
        and address is not null
        and price_in_billions is not null
        and price_in_billions > 0
        and price_in_billions < 1000  -- Outlier removal
        
        -- AREA: Can be NULL (many listings don't have area), but if exists must be valid
        and (area is null or (area > 0 and area < 10000))
        
        -- OPTIONAL FIELDS: Can be NULL (will handle in SELECT)
        -- bedrooms, bathrooms, floors, house_direction, etc.
),

deduplicated_properties as (
    select * from properties where rn = 1  -- Keep only latest
),

locations as (
    select * from {{ ref('dim_locations') }}
),

legal_statuses as (
    select * from {{ ref('dim_legal_status') }}
)

select
    -- Fact primary key
    p.property_id,
    
    -- Foreign keys to dimensions
    l.location_id,  -- Guaranteed NOT NULL (filtered in WHERE)
    ls.legal_status_id,  -- Guaranteed NOT NULL (filtered in WHERE)
    cast(date_trunc('day', p.updated_at_ts) as date) as date_key,
    
    -- MEASURES: Critical metrics (guaranteed NOT NULL by WHERE filters)
    p.price_in_billions,
    p.area,
    round((p.price_in_billions * 1000) / p.area, 3) as price_per_m2_millions,
    
    -- OPTIONAL ATTRIBUTES: Keep NULL as-is (shows data incompleteness)
    -- Queries should use COALESCE() or WHERE IS NOT NULL when needed
    p.floors,  -- Can be NULL
    p.bedrooms,  -- Can be NULL
    p.bathrooms,  -- Can be NULL
    p.house_direction,  -- Can be NULL
    
    -- Degenerate dimensions
    p.title,  -- Guaranteed NOT NULL (filtered in WHERE)
    p.images,
    
    -- Timestamps
    p.updated_at_ts,
    p.created_at

from deduplicated_properties p

-- Join with location dimension
inner join locations l
    on p.address = l.full_address

-- Join with legal status dimension  
inner join legal_statuses ls
    on coalesce(p.legal_status, 'Không xác định') = ls.legal_status

-- NO additional WHERE filter needed
-- All records here are complete and join successfully