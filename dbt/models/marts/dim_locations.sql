{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='full_address',
    incremental_strategy='merge'
) }}

-- Dimension table: Locations
-- Chuẩn hóa địa chỉ thành hierarchy (Province -> District -> Ward)
-- ✅ FIXED: Changed to incremental to preserve location_id across runs

with parsed_locations as (
    select 
        coalesce(address, 'Unknown') as address,  -- ✅ Handle NULL addresses (FIXED: added comma)
        
        -- Extract location components from address
        case 
            when address is null then 'Unknown'
            when address like '%Hồ Chí Minh%' or address like '%TP. HCM%' or address like '%Sài Gòn%' 
                then 'Hồ Chí Minh'
            when address like '%Hà Nội%' or address like '%Hanoi%'
                then 'Hà Nội'
            when address like '%Đà Nẵng%' or address like '%Da Nang%'
                then 'Đà Nẵng'
            when address like '%Bình Dương%'
                then 'Bình Dương'
            when address like '%Đồng Nai%'
                then 'Đồng Nai'
            when address like '%Cần Thơ%'
                then 'Cần Thơ'
            when address like '%Hải Phòng%'
                then 'Hải Phòng'
            else 'Khác'
        end as province,
        
        -- Extract district (simplified - should be enhanced)
        case 
            when address is null then 'Unknown'
            when address like '%Quận 1%' then 'Quận 1'
            when address like '%Quận 2%' then 'Quận 2'
            when address like '%Quận 3%' then 'Quận 3'
            when address like '%Quận 4%' then 'Quận 4'
            when address like '%Quận 5%' then 'Quận 5'
            when address like '%Quận 6%' then 'Quận 6'
            when address like '%Quận 7%' then 'Quận 7'
            when address like '%Quận 8%' then 'Quận 8'
            when address like '%Quận 9%' then 'Quận 9'
            when address like '%Quận 10%' then 'Quận 10'
            when address like '%Quận 11%' then 'Quận 11'
            when address like '%Quận 12%' then 'Quận 12'
            when address like '%Thủ Đức%' then 'Thủ Đức'
            when address like '%Bình Thạnh%' then 'Bình Thạnh'
            when address like '%Gò Vấp%' then 'Gò Vấp'
            when address like '%Phú Nhuận%' then 'Phú Nhuận'
            when address like '%Tân Bình%' then 'Tân Bình'
            when address like '%Tân Phú%' then 'Tân Phú'
            when address like '%Bình Tân%' then 'Bình Tân'
            else 'Khác'
        end as district
        
    from {{ ref('stg_properties') }}
    where 
        data_quality_flag = 'VALID'
        -- ✅ Don't filter out NULL addresses, we'll handle them
)

select
    -- ✅ FIXED: Use hash-based ID instead of row_number() to ensure stability across runs
    abs(hash(address)) % 2147483647 as location_id,  -- Generate stable ID from address
    address as full_address,
    province,
    district,
    null as ward,  -- Can be enhanced with more parsing
    
    -- Categorize by region
    case 
        when province in ('Hồ Chí Minh', 'Bình Dương', 'Đồng Nai', 'Vũng Tàu') 
            then 'Miền Nam'
        when province in ('Hà Nội', 'Hải Phòng', 'Quảng Ninh')
            then 'Miền Bắc'
        when province = 'Đà Nẵng'
            then 'Miền Trung'
        else 'Khác'
    end as region

from parsed_locations
group by 
    address,
    province,
    district,
    region  -- ✅ Removed ward from GROUP BY (it's always NULL)
