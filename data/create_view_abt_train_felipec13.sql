CREATE OR REPLACE VIEW sales_analytics.view_abt_train_felipec13 AS
WITH filtered_data AS (
    SELECT 
        store_id,
        CAST(price AS DECIMAL(10, 2)) AS total_sales,
        CAST(date_sale AS DATE) AS date
    FROM 
        sales.item_sale
    WHERE 
        date_sale < CURRENT_DATE
),
aggregated_data AS (
    SELECT
        store_id,
        date,
        SUM(total_sales) AS total_sales
    FROM 
        filtered_data
    GROUP BY 
        store_id, date
),
split_date AS (
    SELECT
        store_id,
        total_sales,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(DAY FROM date) AS day,
        EXTRACT(DOW FROM date) AS weekday
    FROM 
        aggregated_data
)
SELECT
    store_id,
    total_sales,
    year,
    month,
    day,
    CASE 
        WHEN weekday = 0 THEN 6 
        ELSE weekday - 1 
    END AS weekday
FROM 
    split_date;
