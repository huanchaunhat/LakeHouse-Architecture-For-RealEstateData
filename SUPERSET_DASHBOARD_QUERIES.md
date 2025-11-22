# 📊 Superset Dashboard Queries - Real Estate Analytics

> **Nguồn dữ liệu:** Spark Thrift Server → MinIO Delta Lake (Gold Layer)  
> **Cập nhật:** Tự động khi refresh dashboard (không cần export PostgreSQL)

---

## ⚠️ QUAN TRỌNG: Custom SQL vs Dataset

### **Cách tạo chart trong Superset có 2 phương pháp:**

#### **Method 1: Custom SQL (Nhanh, đơn giản)**

```
SQL Lab → Run query → Save → Save dataset → Create chart
```

✅ **Support:** Table, Bar Chart, Line Chart, Pie Chart, Scatter Plot, Box Plot  
❌ **Không support:** Bubble Chart, Heatmap, Treemap, Sunburst

#### **Method 2: Virtual Dataset (Linh hoạt hơn, support tất cả chart types)**

```
SQL Lab → Run query → Save → Save dataset → Charts → Choose dataset → Select chart type
```

✅ **Support:** Tất cả chart types, kể cả Bubble Chart, Heatmap

### **💡 Khuyến Nghị:**

- **Charts đơn giản** (Table, Bar, Line, Pie) → Dùng Custom SQL
- **Charts phức tạp** (Bubble, Heatmap) → Phải tạo Dataset trước
- Nếu gặp lỗi "**Chart type requires a dataset**" → Save query as dataset trước

---

## 🎯 Tổng Quan Dashboard

### **Dashboard 1: Executive Summary** (Tổng Quan Điều Hành)

- 4 KPI Cards (tổng số, giá TB, tổng giá trị, tăng trưởng)
- 1 Line Chart (xu hướng giá theo thời gian)
- 1 Bar Chart (top tỉnh thành)
- 1 Pie Chart (phân bố theo tình trạng pháp lý)

### **Dashboard 2: Market Analysis** (Phân Tích Thị Trường)

- Geographic heatmap (giá theo khu vực)
- Price comparison (so sánh giá giữa các tỉnh)
- Supply/Demand trends
- Hottest markets

### **Dashboard 3: Property Intelligence** (Thông Tin Bất Động Sản)

- Property size distribution
- Bedrooms/Bathrooms analysis
- Price per m² analysis
- Legal status breakdown

### **Dashboard 4: Time Series** (Phân Tích Thời Gian)

- Daily/Weekly/Monthly trends
- Year-over-Year comparison
- Seasonal patterns
- Growth rates

---

## 📈 Dashboard 1: Executive Summary

### **KPI Card 1: Tổng Số Tin Đăng**

```sql
-- Total Listings (All Time)
SELECT
    COUNT(*) as total_listings,
    COUNT(DISTINCT property_id) as unique_properties
FROM gold.fct_properties;
```

**Chart Type:** Big Number  
**Metric:** `total_listings`  
**Subheader:** "Total Properties Listed"

---

### **KPI Card 2: Giá Trung Bình**

```sql
-- Average Price & Price per m²
SELECT
    ROUND(AVG(price_in_billions), 2) as avg_price_billion,
    ROUND(AVG(price_per_m2_millions), 2) as avg_price_per_m2,
    ROUND(PERCENTILE(price_in_billions, 0.5), 2) as median_price
FROM gold.fct_properties
WHERE price_in_billions IS NOT NULL;
```

**Chart Type:** Big Number with Trendline  
**Metric:** `avg_price_billion`  
**Subheader:** "Billion VND"

---

### **KPI Card 3: Tổng Giá Trị Thị Trường**

```sql
-- Total Market Value
SELECT
    ROUND(SUM(price_in_billions), 2) as total_value_billions,
    CONCAT(ROUND(SUM(price_in_billions) / 1000, 1), ' Trillion VND') as total_value_formatted
FROM gold.fct_properties
WHERE price_in_billions IS NOT NULL;
```

**Chart Type:** Big Number  
**Metric:** `total_value_billions`  
**Subheader:** "Total Market Value (Billion VND)"

---

### **KPI Card 4: Tăng Trưởng 30 Ngày**

```sql
-- 30-Day Growth Rate
WITH current_period AS (
    SELECT COUNT(*) as current_count
    FROM gold.fct_properties
    WHERE created_at >= DATE_SUB(CURRENT_DATE(), 30)
),
previous_period AS (
    SELECT COUNT(*) as previous_count
    FROM gold.fct_properties
    WHERE created_at >= DATE_SUB(CURRENT_DATE(), 60)
      AND created_at < DATE_SUB(CURRENT_DATE(), 30)
)
SELECT
    c.current_count as new_listings_30d,
    ROUND(((c.current_count - p.previous_count) * 100.0 / NULLIF(p.previous_count, 0)), 1) as growth_rate_pct
FROM current_period c, previous_period p;
```

**Chart Type:** Big Number with Trend Arrow  
**Metric:** `growth_rate_pct`  
**Subheader:** "30-Day Growth Rate %"

---

### **Chart 1: Xu Hướng Giá Theo Thời Gian**

```sql
-- Price Trends Over Time (Last 90 Days)
SELECT
    DATE(created_at) as date,
    COUNT(*) as daily_listings,
    ROUND(AVG(price_in_billions), 2) as avg_price,
    ROUND(AVG(price_per_m2_millions), 2) as avg_price_per_m2,
    ROUND(AVG(area), 2) as avg_area
FROM gold.fct_properties
WHERE created_at >= DATE_SUB(CURRENT_DATE(), 90)
  AND price_in_billions IS NOT NULL
GROUP BY DATE(created_at)
ORDER BY date;
```

**Chart Type:** Line Chart (Multiple Metrics)  
**X-axis:** `date`  
**Metrics:**

- `avg_price` (Primary axis, blue line)
- `daily_listings` (Secondary axis, bar chart, light gray)
- `avg_price_per_m2` (Primary axis, orange line)

**Settings:**

- Show data labels: No
- Show legend: Yes (top)
- Smooth lines: Yes
- Time format: `%d/%m`

---

### **Chart 2: Top 15 Tỉnh/Thành Phố**

```sql
-- Top 15 Provinces by Number of Listings
SELECT
    l.province,
    COUNT(*) as total_properties,
    ROUND(AVG(f.price_in_billions), 2) as avg_price,
    ROUND(AVG(f.price_per_m2_millions), 2) as avg_price_m2,
    ROUND(AVG(f.area), 2) as avg_area,
    ROUND(SUM(f.price_in_billions), 2) as total_value
FROM gold.fct_properties f
JOIN gold.dim_locations l ON f.location_id = l.location_id
WHERE f.price_in_billions IS NOT NULL
GROUP BY l.province
ORDER BY total_properties DESC
LIMIT 15;
```

**Chart Type:** Bar Chart (Horizontal)  
**X-axis:** `province`  
**Y-axis:** `total_properties`  
**Color:** Gradient based on `avg_price`

**Tooltip:**

- Province
- Total Properties: `total_properties`
- Average Price: `avg_price` billion
- Avg Price/m²: `avg_price_m2` million

---

### **Chart 3: Phân Bố Theo Tình Trạng Pháp Lý**

```sql
-- Legal Status Distribution
SELECT
    ls.legal_status,
    ls.legal_status_category,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage,
    ROUND(AVG(f.price_in_billions), 2) as avg_price
FROM gold.fct_properties f
JOIN gold.dim_legal_status ls ON f.legal_status_id = ls.legal_status_id
WHERE f.price_in_billions IS NOT NULL
GROUP BY ls.legal_status, ls.legal_status_category
ORDER BY count DESC;
```

**Chart Type:** Donut Chart  
**Dimension:** `legal_status`  
**Metric:** `count`  
**Color Scheme:** Categorical (distinct colors per status)

**Tooltip:**

- Legal Status: `legal_status`
- Count: `count`
- Percentage: `percentage%`
- Avg Price: `avg_price` billion

---

## 🗺️ Dashboard 2: Market Analysis

### **Chart 4: Bản Đồ Nhiệt Giá Theo Tỉnh**

```sql
-- Price Heatmap by Province
SELECT
    l.province,
    l.region,
    COUNT(*) as total_listings,
    ROUND(AVG(f.price_per_m2_millions), 2) as avg_price_m2,
    ROUND(MIN(f.price_per_m2_millions), 2) as min_price_m2,
    ROUND(MAX(f.price_per_m2_millions), 2) as max_price_m2
FROM gold.fct_properties f
JOIN gold.dim_locations l ON f.location_id = l.location_id
WHERE f.price_per_m2_millions IS NOT NULL
GROUP BY l.province, l.region
HAVING COUNT(*) >= 5
ORDER BY avg_price_m2 DESC;
```

**Chart Type:** Table  
**Columns:**

- `province` (String)
- `region` (String)
- `total_listings` (Number)
- `avg_price_m2` (Number, format: `,d` with 2 decimals)
- `min_price_m2` (Number)
- `max_price_m2` (Number)

**Settings:**

- Enable pagination: Yes
- Page size: 20
- Show totals: No
- Column formatting: Currency format for price columns

**Alternative Chart Type:** Treemap

- Dimensions: `province`, `region`
- Metric: `total_listings` (size)
- Color metric: `avg_price_m2` (Red = high, Green = low)

---

### **Chart 5: So Sánh Giá Giữa Các Miền**

```sql
-- Regional Price Comparison
SELECT
    l.region,
    COUNT(*) as properties,
    ROUND(AVG(f.price_in_billions), 2) as avg_price,
    ROUND(AVG(f.price_per_m2_millions), 2) as avg_price_m2,
    ROUND(AVG(f.area), 2) as avg_area,
    ROUND(STDDEV(f.price_in_billions), 2) as price_std_dev
FROM gold.fct_properties f
JOIN gold.dim_locations l ON f.location_id = l.location_id
WHERE f.price_in_billions IS NOT NULL
GROUP BY l.region
ORDER BY avg_price DESC;
```

**Chart Type:** Bar Chart (Grouped)  
**X-axis:** `region`  
**Metrics:**

- `avg_price` (Primary bar, blue)
- `avg_price_m2` (Secondary bar, orange)

**Settings:**

- Show values on bars: Yes
- Legend position: Top

---

### **Chart 6: Top 10 Quận/Huyện Đắt Nhất**

```sql
-- Top 10 Most Expensive Districts
SELECT
    CONCAT(l.district, ', ', l.province) as location,
    COUNT(*) as properties,
    ROUND(AVG(f.price_per_m2_millions), 2) as avg_price_m2,
    ROUND(AVG(f.price_in_billions), 2) as avg_price,
    ROUND(MAX(f.price_in_billions), 2) as max_price
FROM gold.fct_properties f
JOIN gold.dim_locations l ON f.location_id = l.location_id
WHERE f.price_per_m2_millions IS NOT NULL
GROUP BY l.province, l.district
HAVING COUNT(*) >= 10  -- Only districts with enough data
ORDER BY avg_price_m2 DESC
LIMIT 10;
```

**Chart Type:** Bar Chart (Horizontal)  
**X-axis (Dimension):** `location` (District, Province)  
**Y-axis (Metric):** `avg_price_m2` (Average Price per m²)  
**Color:** Gradient based on `avg_price_m2` (Red = expensive, Green = affordable)

**Tooltip:**

- Location: `location`
- Properties: `properties`
- Avg Price/m²: `avg_price_m2` million VND
- Avg Price: `avg_price` billion VND
- Max Price: `max_price` billion VND

**Settings:**

- Bar orientation: Horizontal
- Show values on bars: Yes
- Sort by: `avg_price_m2` DESC

---

### **Chart 7: Thị Trường Nóng Nhất (Momentum)**

```sql
-- Hottest Markets (Most Active Last 30 Days)
WITH recent_activity AS (
    SELECT
        l.province,
        COUNT(*) as recent_listings,
        ROUND(AVG(f.price_in_billions), 2) as avg_price
    FROM gold.fct_properties f
    JOIN gold.dim_locations l ON f.location_id = l.location_id
    WHERE f.created_at >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY l.province
),
total_activity AS (
    SELECT
        l.province,
        COUNT(*) as total_listings
    FROM gold.fct_properties f
    JOIN gold.dim_locations l ON f.location_id = l.location_id
    GROUP BY l.province
)
SELECT
    r.province,
    r.recent_listings,
    t.total_listings,
    ROUND(r.recent_listings * 100.0 / t.total_listings, 1) as momentum_pct,
    r.avg_price
FROM recent_activity r
JOIN total_activity t ON r.province = t.province
WHERE r.recent_listings >= 10
ORDER BY momentum_pct DESC
LIMIT 15;
```

**⚠️ IMPORTANT: Bubble Chart yêu cầu Dataset (không support Custom SQL)**

**Cách tạo:**

1. **SQL Lab** → Run query trên
2. **Save** → **Save dataset** → Tên: `market_momentum`
3. **Charts** → **+ Chart** → **Bubble Chart**
4. Chọn dataset: `market_momentum`

**Chart Type:** Bubble Chart  
**X-axis (Metric):** `total_listings` (Total Market Size)  
**Y-axis (Metric):** `avg_price` (Average Price)  
**Bubble Size:** `momentum_pct` (Market Momentum %)  
**Series (Dimension):** `province` (one bubble per province)

**Alternative (nếu không muốn tạo dataset):** Dùng **Scatter Plot** với Custom SQL:

```sql
-- Same query, works in Custom SQL
```

- X-axis: `total_listings`
- Y-axis: `avg_price`
- Point Size: `momentum_pct`

---

## 🏠 Dashboard 3: Property Intelligence

### **Chart 8: Phân Bố Diện Tích**

```sql
-- Area Distribution
SELECT
    CASE
        WHEN area < 30 THEN '< 30 m²'
        WHEN area BETWEEN 30 AND 50 THEN '30-50 m²'
        WHEN area BETWEEN 50 AND 80 THEN '50-80 m²'
        WHEN area BETWEEN 80 AND 120 THEN '80-120 m²'
        WHEN area BETWEEN 120 AND 200 THEN '120-200 m²'
        WHEN area >= 200 THEN '200+ m²'
        ELSE 'Unknown'
    END as area_range,
    COUNT(*) as properties,
    ROUND(AVG(price_in_billions), 2) as avg_price,
    ROUND(AVG(price_per_m2_millions), 2) as avg_price_m2
FROM gold.fct_properties
WHERE area IS NOT NULL
GROUP BY area_range
ORDER BY
    CASE area_range
        WHEN '< 30 m²' THEN 1
        WHEN '30-50 m²' THEN 2
        WHEN '50-80 m²' THEN 3
        WHEN '80-120 m²' THEN 4
        WHEN '120-200 m²' THEN 5
        WHEN '200+ m²' THEN 6
        ELSE 7
    END;
```

**Chart Type:** Bar Chart (Vertical)  
**X-axis:** `area_range`  
**Y-axis:** `properties` (count)  
**Color:** Gradient based on `avg_price_m2`

---

### **Chart 9: Phân Tích Số Phòng Ngủ**

```sql
-- Bedrooms Analysis with NULL Handling
SELECT
    CASE
        WHEN bedrooms IS NULL THEN '❓ Không rõ'
        WHEN bedrooms = 0 THEN '🏢 Studio'
        WHEN bedrooms = 1 THEN '🛏️ 1 PN'
        WHEN bedrooms = 2 THEN '🛏️🛏️ 2 PN'
        WHEN bedrooms = 3 THEN '🛏️🛏️🛏️ 3 PN'
        WHEN bedrooms = 4 THEN '🏠 4 PN'
        WHEN bedrooms >= 5 THEN '🏰 5+ PN'
    END as bedroom_category,
    COUNT(*) as properties,
    ROUND(AVG(price_in_billions), 2) as avg_price,
    ROUND(AVG(price_per_m2_millions), 2) as avg_price_m2,
    ROUND(AVG(area), 2) as avg_area
FROM gold.fct_properties
GROUP BY bedroom_category
ORDER BY properties DESC;
```

**Chart Type:** Pie Chart  
**Dimension:** `bedroom_category`  
**Metric:** `properties`  
**Show Percentage:** Yes

---

### **Chart 10: Ma Trận Giá Theo Phòng Ngủ & Diện Tích**

```sql
-- Price Matrix: Bedrooms vs Area
SELECT
    CASE
        WHEN bedrooms = 1 THEN '1 PN'
        WHEN bedrooms = 2 THEN '2 PN'
        WHEN bedrooms = 3 THEN '3 PN'
        WHEN bedrooms >= 4 THEN '4+ PN'
        ELSE 'Other'
    END as bedroom_cat,
    CASE
        WHEN area < 50 THEN '< 50 m²'
        WHEN area BETWEEN 50 AND 80 THEN '50-80 m²'
        WHEN area BETWEEN 80 AND 120 THEN '80-120 m²'
        WHEN area >= 120 THEN '120+ m²'
    END as area_cat,
    COUNT(*) as count,
    ROUND(AVG(price_in_billions), 2) as avg_price
FROM gold.fct_properties
WHERE bedrooms IS NOT NULL AND area IS NOT NULL
GROUP BY bedroom_cat, area_cat
HAVING COUNT(*) >= 5
ORDER BY bedroom_cat, area_cat;
```

**⚠️ IMPORTANT: Heatmap Chart yêu cầu Dataset (không support Custom SQL)**

**Cách tạo:**

1. **SQL Lab** → Run query trên
2. **Save** → **Save dataset** → Tên: `price_matrix_bedroom_area`
3. **Charts** → **+ Chart** → **Heatmap Chart**
4. Chọn dataset: `price_matrix_bedroom_area`

**Chart Type:** Heatmap Chart  
**Rows (Y-axis):** `bedroom_cat` (Bedrooms category)  
**Columns (X-axis):** `area_cat` (Area range)  
**Metric:** `avg_price` (Average Price)  
**Color Scheme:** `schemeRdYlGn_r` (Red = expensive, Yellow = moderate, Green = affordable)

**Settings:**

- Show values: Yes
- Normalize across: Heatmap
- Sort X-axis: Lexicographical
- Sort Y-axis: Lexicographical

**Alternative (nếu không muốn tạo dataset):** Dùng **Pivot Table** với Custom SQL - tương tự heatmap nhưng dạng bảng

---

### **Chart 11: Phân Tích Hướng Nhà**

```sql
-- House Direction Analysis
SELECT
    COALESCE(house_direction, 'Không xác định') as direction,
    COUNT(*) as properties,
    ROUND(AVG(price_in_billions), 2) as avg_price,
    ROUND(AVG(price_per_m2_millions), 2) as avg_price_m2
FROM gold.fct_properties
GROUP BY house_direction
ORDER BY properties DESC;
```

**Chart Type Option 1:** Bar Chart (Horizontal)  
**X-axis:** `direction`  
**Y-axis:** `properties`  
**Color:** Gradient based on `avg_price`

**Chart Type Option 2:** Pie Chart  
**Dimension:** `direction`  
**Metric:** `properties`
**Show Percentage:** Yes

**Note:** Superset không có Radar Chart native. Dùng Bar Chart để so sánh hoặc Pie Chart để xem tỷ lệ.

---

## ⏰ Dashboard 4: Time Series Analysis

### **Chart 12: Xu Hướng Hàng Ngày (30 Ngày Gần Nhất)**

```sql
-- Daily Trends (Last 30 Days) from Pre-aggregated Table
SELECT
    report_date,
    total_new_listings,
    avg_price_per_m2_millions,
    avg_area,
    avg_bedrooms,
    avg_floors
FROM gold.fct_daily_summary
WHERE report_date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY report_date;
```

**Chart Type:** Line Chart (Multiple Lines)  
**X-axis:** `report_date` (Time format: `%d/%m`)  
**Y-axes:**

- Primary: `avg_price_per_m2_millions` (blue line)
- Secondary: `total_new_listings` (bar chart, gray)

---

### **Chart 13: So Sánh Theo Tuần**

```sql
-- Weekly Comparison (Last 12 Weeks)
SELECT
    d.week_of_year,
    d.year,
    COUNT(*) as listings,
    ROUND(AVG(f.price_in_billions), 2) as avg_price,
    ROUND(AVG(f.price_per_m2_millions), 2) as avg_price_m2
FROM gold.fct_properties f
JOIN gold.dim_date d ON DATE(f.created_at) = d.date_day
WHERE f.created_at >= DATE_SUB(CURRENT_DATE(), 84)  -- 12 weeks
GROUP BY d.week_of_year, d.year
ORDER BY d.year, d.week_of_year;
```

**Chart Type:** Bar Chart (Grouped by Week)  
**X-axis:** `week_of_year`  
**Y-axis:** `listings`  
**Tooltip:** Show `avg_price` and `avg_price_m2`

---

### **Chart 14: Xu Hướng Theo Tháng**

```sql
-- Monthly Trends (Last 12 Months)
SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(f.property_id) as listings,
    ROUND(AVG(f.price_in_billions), 2) as avg_price,
    ROUND(AVG(f.area), 2) as avg_area,
    ROUND(SUM(f.price_in_billions), 2) as total_value
FROM gold.fct_properties f
JOIN gold.dim_date d ON DATE(f.created_at) = d.date_day
WHERE f.created_at >= DATE_SUB(CURRENT_DATE(), 365)
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

**Chart Type:** Line Chart + Bar Chart (Combo)  
**X-axis:** `month_name`  
**Y-axes:**

- Primary: `avg_price` (line, blue)
- Secondary: `listings` (bar, light gray)

---

### **Chart 15: So Sánh Năm Trước (YoY)**

```sql
-- Year-over-Year Comparison
WITH current_year AS (
    SELECT
        d.month,
        d.month_name,
        COUNT(*) as listings_2024,
        ROUND(AVG(f.price_in_billions), 2) as avg_price_2024
    FROM gold.fct_properties f
    JOIN gold.dim_date d ON DATE(f.created_at) = d.date_day
    WHERE d.year = 2024
    GROUP BY d.month, d.month_name
),
previous_year AS (
    SELECT
        d.month,
        COUNT(*) as listings_2023,
        ROUND(AVG(f.price_in_billions), 2) as avg_price_2023
    FROM gold.fct_properties f
    JOIN gold.dim_date d ON DATE(f.created_at) = d.date_day
    WHERE d.year = 2023
    GROUP BY d.month
)
SELECT
    c.month,
    c.month_name,
    c.listings_2024,
    p.listings_2023,
    c.avg_price_2024,
    p.avg_price_2023,
    ROUND((c.listings_2024 - p.listings_2023) * 100.0 / NULLIF(p.listings_2023, 0), 1) as listings_growth_pct,
    ROUND((c.avg_price_2024 - p.avg_price_2023) * 100.0 / NULLIF(p.avg_price_2023, 0), 1) as price_growth_pct
FROM current_year c
LEFT JOIN previous_year p ON c.month = p.month
ORDER BY c.month;
```

**Chart Type:** Line Chart (Comparison)  
**X-axis:** `month_name`  
**Lines:**

- `listings_2024` (solid blue)
- `listings_2023` (dashed gray)

**Tooltip:**

- Month
- 2024: `listings_2024` / `avg_price_2024`
- 2023: `listings_2023` / `avg_price_2023`
- Growth: `listings_growth_pct%`

---

## 🔍 Advanced Queries

### **Chart 16: Outlier Detection (Giá Bất Thường)**

```sql
-- Find Outliers (Unusually High/Low Prices)
WITH stats AS (
    SELECT
        AVG(price_per_m2_millions) as avg_price,
        STDDEV(price_per_m2_millions) as std_dev
    FROM gold.fct_properties
    WHERE price_per_m2_millions IS NOT NULL
)
SELECT
    f.property_id,
    f.title,
    l.province,
    l.district,
    f.price_in_billions,
    f.price_per_m2_millions,
    f.area,
    CASE
        WHEN f.price_per_m2_millions > s.avg_price + (2 * s.std_dev) THEN 'High Outlier'
        WHEN f.price_per_m2_millions < s.avg_price - (2 * s.std_dev) THEN 'Low Outlier'
        ELSE 'Normal'
    END as outlier_type
FROM gold.fct_properties f
JOIN gold.dim_locations l ON f.location_id = l.location_id
CROSS JOIN stats s
WHERE f.price_per_m2_millions IS NOT NULL
  AND (f.price_per_m2_millions > s.avg_price + (2 * s.std_dev)
   OR f.price_per_m2_millions < s.avg_price - (2 * s.std_dev))
ORDER BY f.price_per_m2_millions DESC
LIMIT 50;
```

**Chart Type:** Scatter Plot  
**X-axis:** `area`  
**Y-axis:** `price_per_m2_millions`  
**Color:** `outlier_type` (Red for high, Blue for low)

---

### **Chart 17: Data Quality Dashboard**

```sql
-- Overall Data Completeness
SELECT
    'Total Properties' as metric,
    COUNT(*) as value,
    ROUND(100.0, 1) as completeness_pct
FROM gold.fct_properties

UNION ALL

SELECT
    'With Price Info' as metric,
    COUNT(*) as value,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM gold.fct_properties), 1) as completeness_pct
FROM gold.fct_properties
WHERE price_in_billions IS NOT NULL

UNION ALL

SELECT
    'With Bedrooms Info' as metric,
    COUNT(*) as value,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM gold.fct_properties), 1) as completeness_pct
FROM gold.fct_properties
WHERE bedrooms IS NOT NULL

UNION ALL

SELECT
    'With Bathrooms Info' as metric,
    COUNT(*) as value,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM gold.fct_properties), 1) as completeness_pct
FROM gold.fct_properties
WHERE bathrooms IS NOT NULL

UNION ALL

SELECT
    'With Floors Info' as metric,
    COUNT(*) as value,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM gold.fct_properties), 1) as completeness_pct
FROM gold.fct_properties
WHERE floors IS NOT NULL;
```

**Chart Type:** Bar Chart (Horizontal)  
**X-axis (Dimension):** `metric`  
**Y-axis (Metric):** `completeness_pct` (Percentage)  
**Color:** Gradient (Green = 100%, Red = low)

**Settings:**

- Show values on bars: Yes (show `completeness_pct%`)
- Y-axis range: 0-100
- Bar orientation: Horizontal
- Sort by: `metric` (manual order)

---

## 🎨 Dashboard Layouts

### **Layout 1: Executive Summary**

```
┌─────────────────────────────────────────────────────────────────┐
│  [KPI 1]         [KPI 2]         [KPI 3]         [KPI 4]       │
│  Total Listings  Avg Price       Total Value     Growth 30d    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│              📈 Price Trends Over Time (Line Chart)             │
│                                                                  │
├───────────────────────────┬─────────────────────────────────────┤
│                           │                                     │
│  📊 Top 15 Provinces      │  🥧 Legal Status Distribution      │
│  (Bar Chart)              │  (Donut Chart)                      │
│                           │                                     │
└───────────────────────────┴─────────────────────────────────────┘
```

### **Layout 2: Market Analysis**

```
┌─────────────────────────────────────────────────────────────────┐
│  🗺️ Price Heatmap by Province (Table with Conditional Format)  │
├───────────────────────────┬─────────────────────────────────────┤
│  📊 Regional Comparison   │  🔥 Hottest Markets                 │
│  (Grouped Bar Chart)      │  (Bubble Chart)                     │
├───────────────────────────┴─────────────────────────────────────┤
│  💰 Top 10 Most Expensive Districts (Table)                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Dashboard Refresh Strategy

### **Auto-refresh Settings**

```
Dashboard → Edit → Settings
├── Auto-refresh interval: 5 minutes (for real-time monitoring)
├── Cache timeout: 3600 seconds (1 hour)
└── Enable async queries: Yes
```

### **Manual Refresh**

- Click **"Refresh"** button (↻) in top-right
- Shortcut: `Ctrl/Cmd + R`

### **Clear Cache**

```sql
-- In SQL Lab, run:
REFRESH TABLE gold.fct_properties;
REFRESH TABLE gold.fct_daily_summary;
```

---

## ✅ Implementation Checklist

- [ ] Connect Superset to Spark Thrift Server (`hive://spark-thrift-server:10000?auth=NOSASL`)
- [ ] Create virtual datasets for all Gold tables
- [ ] Create 4 KPI cards for Executive Summary
- [ ] Build 3 main charts per dashboard (total 12+ charts)
- [ ] Configure auto-refresh (5 minutes)
- [ ] Set up filters (Province, Date Range, Property Type)
- [ ] Test all queries return data
- [ ] Configure chart tooltips with relevant metrics
- [ ] Add dashboard descriptions and help text

---

## 🎨 Superset Chart Types Cheat Sheet

### **Available Chart Types trong Superset 3.0**

| Chart Type                    | Khi nào dùng                     | Ví dụ                               |
| ----------------------------- | -------------------------------- | ----------------------------------- |
| **Big Number**                | KPI cards, single metrics        | Total Listings, Avg Price           |
| **Big Number with Trendline** | KPI with historical trend        | Revenue growth, Price trends        |
| **Table**                     | Detail data, multiple columns    | Property listings, Price comparison |
| **Pivot Table**               | Cross-tab analysis               | Bedrooms × Area price matrix        |
| **Bar Chart**                 | Compare categories               | Top provinces, Districts by price   |
| **Line Chart**                | Time series, trends              | Daily price, Monthly listings       |
| **Area Chart**                | Cumulative trends                | Total value over time               |
| **Pie Chart**                 | Distribution, percentages        | Legal status, Bedroom distribution  |
| **Donut Chart**               | Distribution (prettier than pie) | Property types, Regions             |
| **Scatter Plot**              | Correlation, outliers            | Price vs Area, Outlier detection    |
| **Bubble Chart**              | 3D data (X, Y, Size)             | Market momentum (size = activity)   |
| **Heatmap Chart**             | 2D matrix with color             | Bedrooms × Area price matrix        |
| **Treemap**                   | Hierarchical data                | Province → District breakdown       |
| **Sunburst**                  | Multi-level hierarchy            | Region → Province → District        |
| **Box Plot**                  | Statistical distribution         | Price distribution by province      |
| **Time-series Line Chart**    | Time-based trends                | Stock-like price movements          |
| **Mixed Time Series**         | Multiple metrics + time          | Price (line) + Listings (bar)       |

### **❌ Chart Types KHÔNG CÓ trong Superset**

- ❌ Radar Chart → Dùng **Bar Chart** or **Pie Chart** thay thế
- ❌ Progress Bar → Dùng **Bar Chart** với custom color gradient
- ❌ Gauge Chart → Dùng **Big Number** hoặc custom echarts
- ❌ Conditional Formatting Tables → Dùng **Table** với color rules (limited)

---

## 🔴 Chart Types & Custom SQL Support

### **✅ Support Custom SQL (có thể dùng query trực tiếp)**

- ✅ **Big Number** - KPI cards
- ✅ **Big Number with Trendline**
- ✅ **Table** - Hiển thị data dạng bảng
- ✅ **Pivot Table** - Cross-tab
- ✅ **Bar Chart** - So sánh categories
- ✅ **Line Chart** - Time series
- ✅ **Area Chart** - Cumulative trends
- ✅ **Pie Chart** - Distribution
- ✅ **Scatter Plot** - Correlation, outliers
- ✅ **Box Plot** - Statistical distribution

### **❌ YÊU CẦU Dataset (KHÔNG support Custom SQL)**

- ❌ **Bubble Chart** → Phải tạo dataset trước
- ❌ **Heatmap Chart** → Phải tạo dataset trước
- ❌ **Treemap** → Phải tạo dataset trước
- ❌ **Sunburst** → Phải tạo dataset trước
- ❌ **Mixed Time Series** → Phải tạo dataset trước

### **🔧 Workaround cho Bubble Chart**

**Option 1: Tạo Dataset (Recommended)**

```
1. SQL Lab → Run query
2. Save → Save dataset
3. Charts → Create chart với dataset
```

**Option 2: Dùng Scatter Plot (Custom SQL OK)**

```sql
-- Same query, nhưng dùng Scatter Plot thay vì Bubble
-- Scatter Plot support Custom SQL và có Point Size
```

**Option 3: Dùng Bar Chart thay thế**

```sql
-- Simple bar chart with tooltip showing all metrics
SELECT province, momentum_pct, avg_price, total_listings
FROM ...
ORDER BY momentum_pct DESC;
```

---

## 📚 Tips & Best Practices

### **Performance Optimization**

1. ✅ Use `gold.fct_daily_summary` for time-based queries (pre-aggregated)
2. ✅ Add `LIMIT` clause for large result sets
3. ✅ Use `HAVING COUNT(*) >= X` to filter small groups
4. ✅ Add WHERE filters to reduce data scanned

### **Query Best Practices**

1. ✅ Always handle NULL with `COALESCE()` or `IS NOT NULL`
2. ✅ Use `ROUND()` for price/area decimals
3. ✅ Add meaningful aliases for all columns
4. ✅ Use CASE statements for categorization

### **Visualization Best Practices**

1. ✅ Choose right chart type (bar for comparison, line for trends, pie for distribution)
2. ✅ Use heatmaps for large tables with numeric values
3. ✅ Add tooltips with context
4. ✅ Use consistent color schemes across dashboard

---

**Last Updated:** November 21, 2025  
**Status:** ✅ Production-Ready Queries  
**Data Source:** Spark Thrift Server → MinIO Delta Lake (Gold Layer)  
**Refresh:** Automatic on dashboard refresh (no PostgreSQL export needed)
