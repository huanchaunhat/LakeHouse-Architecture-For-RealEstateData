{{ config(
    materialized='table',
    file_format='delta'
) }}

-- Dimension table: Date
-- Standard date dimension for time-based analysis

with date_spine as (
    -- Generate dates for last 3 years and next 1 year
    select 
        explode(sequence(
            to_date('2023-01-01'),
            to_date('2026-12-31'),
            interval 1 day
        )) as date_day
)

select
    date_day,
    
    -- Date components
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    dayofmonth(date_day) as day,
    dayofweek(date_day) as day_of_week,
    dayofyear(date_day) as day_of_year,
    weekofyear(date_day) as week_of_year,
    
    -- Date names
    date_format(date_day, 'MMMM') as month_name,
    date_format(date_day, 'E') as day_name,
    
    -- Flags
    case when dayofweek(date_day) in (1, 7) then true else false end as is_weekend,
    case when month(date_day) = month(current_date()) 
         and year(date_day) = year(current_date()) 
         then true else false end as is_current_month,
    
    -- Fiscal periods (assuming fiscal year = calendar year)
    concat('Q', quarter(date_day), ' ', year(date_day)) as quarter_name,
    concat(year(date_day), '-', lpad(month(date_day), 2, '0')) as year_month

from date_spine
