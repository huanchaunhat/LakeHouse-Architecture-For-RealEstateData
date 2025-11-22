-- Bảng báo cáo chất lượng dữ liệu
-- Giúp monitor và phát hiện vấn đề trong pipeline

select
    date_trunc('day', updated_at_ts) as report_date,
    data_quality_flag,
    count(*) as record_count,
    count(*) * 100.0 / sum(count(*)) over (partition by date_trunc('day', updated_at_ts)) as percentage

from {{ ref('stg_properties') }}

group by 1, 2
order by 1 desc, 2
