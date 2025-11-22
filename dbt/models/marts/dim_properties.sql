{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='property_id',
    incremental_strategy='merge'
) }}

-- Dimension table: Property Attributes
-- Chứa thông tin thuộc tính của property (slowly changing dimension type 1 - overwrite)
-- ✅ FIXED: Changed to incremental to update attributes when they change

with deduplicated as (
    select
        property_id,
        title,  -- ✅ Keep NULL as-is for optional fields
        
        -- Physical attributes - KEEP NULL (don't replace with 0)
        area,  -- Critical field - will be filtered in fact table
        frontage,  -- Optional field - can be NULL
        floors,  -- Optional field - can be NULL
        bedrooms,  -- Optional field - can be NULL
        bathrooms,  -- Optional field - can be NULL
        
        -- Property characteristics
        legal_status,  -- Will be handled via dimension lookup
        house_direction,  -- Optional - can be NULL
        
        -- Metadata
        created_at,
        updated_at_ts,
        
        -- SCD Type 2 fields
        updated_at_ts as valid_from,
        true as is_current,
        
        -- For deduplication: use row_number to keep latest
        row_number() over (
            partition by property_id 
            order by updated_at_ts desc, created_at desc
        ) as rn
        
    from {{ ref('stg_properties') }}
    where 
        data_quality_flag = 'VALID'
        -- ✅ Filter critical NULLs here
        and property_id is not null
        and title is not null
)

select
    property_id,
    title,
    area,
    frontage,
    floors,
    bedrooms,
    bathrooms,
    legal_status,
    house_direction,
    created_at,
    updated_at_ts,
    valid_from,
    is_current

from deduplicated
where rn = 1  -- ✅ Keep only latest record per property_id
