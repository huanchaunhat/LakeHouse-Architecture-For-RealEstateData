select
    -- Chọn các cột cần thiết cho nghiệp vụ
    property_id,
    title,
    area,
    price_in_billions,
    address,
    floors,
    bedrooms,
    bathrooms,
    legal_status,
    house_direction,
    updated_at_ts,

    -- Thêm logic tính toán
    -- (giá Tỷ * 1000) / diện tích = giá Triệu / m2
    round((price_in_billions * 1000) / area, 3) as price_per_m2_millions

from {{ ref('stg_properties') }} -- Đọc từ model Silver

-- Áp dụng bộ lọc nghiệp vụ
where
    area > 0
    and price_in_billions > 0