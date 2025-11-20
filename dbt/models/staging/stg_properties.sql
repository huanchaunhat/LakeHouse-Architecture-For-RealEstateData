{{ config(
    materialized='table',
    file_format='delta'
) }}

with source_data as (
    
    select * from {{ source('raw_lakehouse', 'raw_properties') }}

),

deduplicated as (
    -- Bước này loại bỏ các bản ghi trùng lặp
    -- Chỉ giữ lại bản ghi MỚI NHẤT cho mỗi list_id
    select
        *,
        row_number() over (
            partition by list_id 
            order by file_modification_time desc
        ) as rn
        
    from source_data
)

-- Logic transform (lấy từ silver.py)
select
    -- Thông tin cơ bản
    list_id as property_id,
    title,
    images,
    file_modification_time as updated_at_ts, -- Lấy thời gian file mới nhất

    -- 1. Chuẩn hóa Area, Numbers (thay thế UDF)
    cast(regexp_extract(`Diện tích đất`, r'([\d,.]+)', 1) as double) as area,
    cast(regexp_extract(`Chiều ngang`, r'([\d,.]+)', 1) as double) as frontage,
    cast(regexp_extract(`Tổng số tầng`, r'(\d+)', 1) as int) as floors,
    cast(regexp_extract(`Số phòng ngủ`, r'(\d+)', 1) as int) as bedrooms,
    cast(regexp_extract(`Số phòng vệ sinh`, r'(\d+)', 1) as int) as bathrooms,

    -- 2. Chuẩn hóa Price (thay thế UDF)
    case
        when lower(price) like '%tỷ%' then cast(regexp_extract(price, r'([\d,.]+)', 1) as double)
        when lower(price) like '%triệu%' then cast(regexp_extract(price, r'([\d,.]+)', 1) as double) / 1000
        else cast(regexp_extract(price, r'([\d,.]+)', 1) as double)
    end as price_in_billions, -- Đơn vị: Tỷ VNĐ

    -- 3. Chuẩn hóa Address (thay thế UDF)
    initcap(trim(
        coalesce(address, `Địa chỉ`, 
            concat_ws(', ', `Phường, thị xã, thị trấn`, `Quận, Huyện`, `Tỉnh, thành phố`)
        )
    )) as address,

    -- 4. Chuẩn hóa cột Text
    initcap(trim(`Giấy tờ pháp lý`)) as legal_status,
    initcap(trim(`Hướng cửa chính`)) as house_direction

from deduplicated

-- Chỉ chọn bản ghi mới nhất (không trùng lặp)
where rn = 1