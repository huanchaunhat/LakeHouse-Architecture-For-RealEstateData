{{ config(
    materialized='incremental',
    file_format='delta',
    unique_key='legal_status',
    incremental_strategy='merge'
) }}

-- Dimension table: Legal Status
-- Lookup table cho tình trạng pháp lý
-- ✅ FIXED: Changed to incremental to preserve legal_status_id

with unique_statuses as (
    select distinct
        coalesce(legal_status, 'Không xác định') as legal_status  -- ✅ Handle NULL
    from {{ ref('stg_properties') }}
    where 
        data_quality_flag = 'VALID'
    
    union  -- ✅ FIXED: Changed from UNION ALL to UNION to deduplicate
    
    -- ✅ Add default "Unknown" status for records without legal_status
    select 'Không xác định' as legal_status
)

select
    -- ✅ FIXED: Use hash-based ID instead of row_number() for stability
    abs(hash(legal_status)) % 2147483647 as legal_status_id,
    legal_status,
    
    -- Categorize legal status
    case 
        when lower(legal_status) like '%sổ đỏ%' or lower(legal_status) like '%sổ hồng%'
            then 'Có sổ đỏ/hồng'
        when lower(legal_status) like '%sổ riêng%'
            then 'Có sổ riêng'
        when lower(legal_status) like '%đang chờ%' or lower(legal_status) like '%chưa có%'
            then 'Chưa có sổ'
        when legal_status is null
            then 'Không rõ'
        else 'Khác'
    end as legal_status_category,
    
    -- Description
    case 
        when lower(legal_status) like '%sổ đỏ%' or lower(legal_status) like '%sổ hồng%'
            then 'Có giấy tờ pháp lý đầy đủ, sổ đỏ/hồng'
        when lower(legal_status) like '%sổ riêng%'
            then 'Có sổ riêng, pháp lý rõ ràng'
        when lower(legal_status) like '%đang chờ%' or lower(legal_status) like '%chưa có%'
            then 'Đang chờ cấp sổ hoặc chưa có sổ'
        else 'Tình trạng pháp lý khác'
    end as description

from unique_statuses
