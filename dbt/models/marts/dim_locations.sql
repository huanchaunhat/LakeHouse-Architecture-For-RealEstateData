{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='full_address',
    incremental_strategy='merge'
) }}

-- Dimension table: Locations
-- Uses structured location data from API (ward, district, province)
-- These are parsed from Chotot API parameters

with location_data as (
    select 
        coalesce(address, 'Unknown') as address,
        coalesce(ward, 'Unknown') as ward,
        coalesce(district, 'Unknown') as district,
        coalesce(province, 'Unknown') as province
        
    from {{ ref('stg_properties') }}
    where 
        data_quality_flag = 'VALID'
        
    {% if is_incremental() %}
    -- Chỉ process locations từ properties MỚI
    and address not in (select full_address from {{ this }})
    {% endif %}
),

-- Deduplicate by address
unique_locations as (
    select 
        address,
        ward,
        district,
        province
    from location_data
    group by 
        address,
        ward,
        district,
        province
)

select
    -- Generate stable ID from full address
    abs(hash(address)) % 2147483647 as location_id,
    address as full_address,
    ward,
    district,
    province,
    
    -- Categorize by region based on province
    case 
        when province in ('Hồ Chí Minh', 'Bình Dương', 'Đồng Nai', 'Bà Rịa - Vũng Tàu', 'Long An', 
                         'Tiền Giang', 'Cần Thơ', 'An Giang', 'Bến Tre', 'Vĩnh Long', 'Đồng Tháp',
                         'Trà Vinh', 'Sóc Trăng', 'Bạc Liêu', 'Cà Mau', 'Kiên Giang', 'Hậu Giang',
                         'Tây Ninh', 'Bình Phước')
            then 'Miền Nam'
        when province in ('Hà Nội', 'Hải Phòng', 'Quảng Ninh', 'Hải Dương', 'Hưng Yên', 'Bắc Ninh',
                         'Bắc Giang', 'Thái Nguyên', 'Lạng Sơn', 'Cao Bằng', 'Hà Giang', 'Tuyên Quang',
                         'Yên Bái', 'Lào Cai', 'Điện Biên', 'Lai Châu', 'Sơn La', 'Hòa Bình',
                         'Phú Thọ', 'Vĩnh Phúc', 'Ninh Bình', 'Nam Định', 'Thái Bình')
            then 'Miền Bắc'
        when province in ('Đà Nẵng', 'Quảng Nam', 'Quảng Ngãi', 'Bình Định', 'Phú Yên', 'Khánh Hòa',
                         'Ninh Thuận', 'Bình Thuận', 'Huế', 'Thừa Thiên Huế', 'Quảng Trị', 'Quảng Bình',
                         'Hà Tĩnh', 'Nghệ An', 'Thanh Hóa', 'Kon Tum', 'Gia Lai', 'Đắk Lắk', 'Đắk Nông', 'Lâm Đồng')
            then 'Miền Trung'
        when province = 'Unknown'
            then 'Unknown'
        else 'Khác'
    end as region

from unique_locations
