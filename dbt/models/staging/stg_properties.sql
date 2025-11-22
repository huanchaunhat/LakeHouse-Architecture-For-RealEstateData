{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='property_id',
    incremental_strategy='merge'
) }}

with source_data as (
    
    select * from {{ source('raw_lakehouse', 'properties') }}
    
    {% if is_incremental() %}
    where file_modification_time > (select max(updated_at_ts) from {{ this }})
    {% endif %}

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
    current_timestamp() as created_at, -- Thời gian tạo record trong Silver

    -- 1. Chuẩn hóa Area, Numbers (FIX: ưu tiên area_raw và usable_area_raw thay vì land_area_raw)
    -- NOTE: land_area_raw = 100% NULL, area_raw có 39.8% data, usable_area_raw có 25.6% data
    coalesce(
        cast(regexp_replace(regexp_extract(area_raw, r'([\d,.]+)', 1), ',', '.') as double),
        cast(regexp_replace(regexp_extract(usable_area_raw, r'([\d,.]+)', 1), ',', '.') as double),
        cast(regexp_replace(regexp_extract(land_area_raw, r'([\d,.]+)', 1), ',', '.') as double)
    ) as area,
    
    cast(
        regexp_replace(
            regexp_extract(frontage_raw, r'([\d,.]+)', 1),
            ',', '.'
        ) as double
    ) as frontage,
    
    cast(regexp_extract(total_floors_raw, r'(\d+)', 1) as int) as floors,
    cast(regexp_extract(bedrooms_raw, r'(\d+)', 1) as int) as bedrooms,
    cast(regexp_extract(bathrooms_raw, r'(\d+)', 1) as int) as bathrooms,

    -- 2. Chuẩn hóa Price (FIX: xử lý dấu phẩy và giá thỏa thuận)
    case
        when lower(price) like '%tỷ%' then 
            cast(
                regexp_replace(
                    regexp_extract(price, r'([\d,.]+)', 1),
                    ',', '.'
                ) as double
            )
        when lower(price) like '%triệu%' then 
            cast(
                regexp_replace(
                    regexp_extract(price, r'([\d,.]+)', 1),
                    ',', '.'
                ) as double
            ) / 1000
        when lower(price) like '%thỏa thuận%' or lower(price) like '%liên hệ%' then NULL
        else 
            cast(
                regexp_replace(
                    regexp_extract(price, r'([\d,.]+)', 1),
                    ',', '.'
                ) as double
            )
    end as price_in_billions, -- Đơn vị: Tỷ VNĐ

    -- 3. Chuẩn hóa Address (chỉ dùng address field vì ward/district/province đều NULL)
    initcap(trim(address)) as address,

    -- 4. Chuẩn hóa cột Text
    initcap(trim(legal_status_raw)) as legal_status,
    initcap(trim(house_direction_raw)) as house_direction,

    -- 5. Data Quality Flag (đánh dấu records có vấn đề)
    case
        when price is null or lower(price) like '%thỏa thuận%' or lower(price) like '%liên hệ%' then 'MISSING_PRICE'
        when address is null or trim(address) = '' then 'MISSING_ADDRESS'
        else 'VALID'
    end as data_quality_flag

from deduplicated

-- Chỉ chọn bản ghi mới nhất (không trùng lặp)
where rn = 1