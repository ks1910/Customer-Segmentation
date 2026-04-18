-- ============================================================
--  CUSTOMER SEGMENTATION – SQL Project
--  Author  : Kriti Singh
--  Dataset : Brazilian E-Commerce (Olist) – Kaggle
--  Tables  :
--    olist_customers           (customer_id, customer_unique_id, zip, city, state)
--    olist_orders              (order_id, customer_id, status, purchase_ts, ...)
--    olist_order_items         (order_id, product_id, seller_id, price, freight)
--    olist_order_payments      (order_id, payment_type, installments, payment_value)
--    olist_order_reviews       (review_id, order_id, score, comment_title, ...)
--    olist_products            (product_id, category_name, ...)
--    olist_sellers             (seller_id, zip, city, state)
--    olist_geolocation         (zip_prefix, lat, lng, city, state)
-- ============================================================


-- ============================================================
-- SECTION 1: DATABASE & TABLE SETUP
-- ============================================================

CREATE DATABASE IF NOT EXISTS olist_db;
USE olist_db;

CREATE TABLE IF NOT EXISTS olist_customers (
    customer_id         VARCHAR(50) PRIMARY KEY,
    customer_unique_id  VARCHAR(50),
    customer_zip_code   VARCHAR(10),
    customer_city       VARCHAR(100),
    customer_state      CHAR(2)
);

CREATE TABLE IF NOT EXISTS olist_orders (
    order_id                      VARCHAR(50) PRIMARY KEY,
    customer_id                   VARCHAR(50),
    order_status                  VARCHAR(20),
    order_purchase_timestamp      DATETIME,
    order_approved_at             DATETIME,
    order_delivered_carrier_date  DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    FOREIGN KEY (customer_id) REFERENCES olist_customers(customer_id)
);

CREATE TABLE IF NOT EXISTS olist_order_items (
    order_id            VARCHAR(50),
    order_item_id       INT,
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date DATETIME,
    price               DECIMAL(10,2),
    freight_value       DECIMAL(10,2),
    PRIMARY KEY (order_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS olist_order_payments (
    order_id            VARCHAR(50),
    payment_sequential  INT,
    payment_type        VARCHAR(30),
    payment_installments INT,
    payment_value       DECIMAL(10,2),
    PRIMARY KEY (order_id, payment_sequential)
);

CREATE TABLE IF NOT EXISTS olist_order_reviews (
    review_id           VARCHAR(50) PRIMARY KEY,
    order_id            VARCHAR(50),
    review_score        INT,
    review_comment_title VARCHAR(100),
    review_comment_message TEXT,
    review_creation_date   DATETIME,
    review_answer_timestamp DATETIME
);

CREATE TABLE IF NOT EXISTS olist_products (
    product_id                  VARCHAR(50) PRIMARY KEY,
    product_category_name       VARCHAR(100),
    product_name_length         INT,
    product_description_length  INT,
    product_photos_qty          INT,
    product_weight_g            INT,
    product_length_cm           INT,
    product_height_cm           INT,
    product_width_cm            INT
);


-- ============================================================
-- SECTION 2: EXPLORATORY DATA ANALYSIS (EDA)
-- ============================================================

-- 2.1 Dataset overview
SELECT
    (SELECT COUNT(DISTINCT customer_unique_id) FROM olist_customers)  AS unique_customers,
    (SELECT COUNT(*)                           FROM olist_orders)      AS total_orders,
    (SELECT COUNT(*)                           FROM olist_order_items) AS total_items,
    (SELECT MIN(order_purchase_timestamp)      FROM olist_orders)      AS first_order,
    (SELECT MAX(order_purchase_timestamp)      FROM olist_orders)      AS last_order;

-- 2.2 Order status breakdown
SELECT
    order_status,
    COUNT(*)                                    AS order_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM olist_orders), 2) AS pct
FROM olist_orders
GROUP BY order_status
ORDER BY order_count DESC;

-- 2.3 Customers by state (geographic distribution)
SELECT
    customer_state,
    COUNT(DISTINCT customer_unique_id) AS num_customers
FROM olist_customers
GROUP BY customer_state
ORDER BY num_customers DESC
LIMIT 10;

-- 2.4 Payment method preference
SELECT
    payment_type,
    COUNT(*)                          AS usage_count,
    ROUND(AVG(payment_value), 2)      AS avg_payment,
    ROUND(SUM(payment_value), 2)      AS total_revenue
FROM olist_order_payments
GROUP BY payment_type
ORDER BY usage_count DESC;

-- 2.5 Average review score
SELECT
    ROUND(AVG(review_score), 2)  AS avg_review_score,
    COUNT(*)                     AS total_reviews,
    SUM(CASE WHEN review_score >= 4 THEN 1 ELSE 0 END) AS positive_reviews,
    SUM(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END) AS negative_reviews
FROM olist_order_reviews;


-- ============================================================
-- SECTION 3: CUSTOMER PURCHASE BEHAVIOUR
-- ============================================================

-- 3.1 Orders per customer (repeat vs one-time buyers)
SELECT
    order_count,
    COUNT(*) AS num_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM (
    SELECT
        c.customer_unique_id,
        COUNT(o.order_id) AS order_count
    FROM olist_customers c
    JOIN olist_orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
) sub
GROUP BY order_count
ORDER BY order_count;

-- 3.2 Average spend per customer
SELECT
    c.customer_unique_id,
    COUNT(DISTINCT o.order_id)                AS total_orders,
    ROUND(SUM(p.payment_value), 2)            AS total_spend,
    ROUND(AVG(p.payment_value), 2)            AS avg_order_value
FROM olist_customers c
JOIN olist_orders o    ON c.customer_id  = o.customer_id
JOIN olist_order_payments p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_unique_id
ORDER BY total_spend DESC
LIMIT 20;

-- 3.3 Revenue by state
SELECT
    c.customer_state,
    COUNT(DISTINCT c.customer_unique_id)  AS num_customers,
    COUNT(DISTINCT o.order_id)            AS total_orders,
    ROUND(SUM(p.payment_value), 2)        AS total_revenue,
    ROUND(AVG(p.payment_value), 2)        AS avg_order_value
FROM olist_customers c
JOIN olist_orders o          ON c.customer_id = o.customer_id
JOIN olist_order_payments p  ON o.order_id    = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY total_revenue DESC;


-- ============================================================
-- SECTION 4: RFM ANALYSIS (Recency, Frequency, Monetary)
-- ============================================================
-- Reference date: 2018-10-17 (last date in dataset + 1 day)

-- 4.1 Compute raw RFM values per customer
WITH rfm_base AS (
    SELECT
        c.customer_unique_id,
        DATEDIFF('2018-10-17', MAX(o.order_purchase_timestamp)) AS recency_days,
        COUNT(DISTINCT o.order_id)                              AS frequency,
        ROUND(SUM(p.payment_value), 2)                          AS monetary
    FROM olist_customers c
    JOIN olist_orders o          ON c.customer_id = o.customer_id
    JOIN olist_order_payments p  ON o.order_id    = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),

-- 4.2 Score each dimension 1–5 using NTILE
rfm_scores AS (
    SELECT
        customer_unique_id,
        recency_days,
        frequency,
        monetary,
        -- Lower recency = more recent = better score
        NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency    ASC)   AS f_score,
        NTILE(5) OVER (ORDER BY monetary     ASC)   AS m_score
    FROM rfm_base
)

-- 4.3 Combine scores and assign segments
SELECT
    customer_unique_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    CONCAT(r_score, f_score, m_score) AS rfm_cell,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4
            THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3
            THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2
            THEN 'New Customers'
        WHEN r_score >= 3 AND f_score >= 2 AND m_score >= 3
            THEN 'Potential Loyalists'
        WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3
            THEN 'At Risk'
        WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4
            THEN 'Cannot Lose Them'
        WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2
            THEN 'Lost / Hibernating'
        ELSE 'Needs Attention'
    END AS customer_segment
FROM rfm_scores
ORDER BY monetary DESC;

-- 4.4 Segment summary – count and avg metrics
WITH rfm_base AS (
    SELECT
        c.customer_unique_id,
        DATEDIFF('2018-10-17', MAX(o.order_purchase_timestamp)) AS recency_days,
        COUNT(DISTINCT o.order_id)                              AS frequency,
        ROUND(SUM(p.payment_value), 2)                          AS monetary
    FROM olist_customers c
    JOIN olist_orders o          ON c.customer_id = o.customer_id
    JOIN olist_order_payments p  ON o.order_id    = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),
rfm_scores AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency    ASC)  AS f_score,
        NTILE(5) OVER (ORDER BY monetary     ASC)  AS m_score
    FROM rfm_base
),
rfm_labeled AS (
    SELECT *,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score <= 2                   THEN 'New Customers'
            WHEN r_score >= 3 AND f_score >= 2 AND m_score >= 3  THEN 'Potential Loyalists'
            WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3  THEN 'At Risk'
            WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4  THEN 'Cannot Lose Them'
            WHEN r_score <= 2 AND f_score <= 2 AND m_score <= 2  THEN 'Lost / Hibernating'
            ELSE 'Needs Attention'
        END AS customer_segment
    FROM rfm_scores
)
SELECT
    customer_segment,
    COUNT(*)                          AS num_customers,
    ROUND(AVG(recency_days), 1)       AS avg_recency_days,
    ROUND(AVG(frequency), 2)          AS avg_frequency,
    ROUND(AVG(monetary), 2)           AS avg_monetary,
    ROUND(SUM(monetary), 2)           AS total_revenue
FROM rfm_labeled
GROUP BY customer_segment
ORDER BY total_revenue DESC;


-- ============================================================
-- SECTION 5: DEMOGRAPHIC SEGMENTATION
-- ============================================================

-- 5.1 Segment customers by state + avg spend bucket
SELECT
    c.customer_state,
    CASE
        WHEN AVG(p.payment_value) >= 300 THEN 'High Value'
        WHEN AVG(p.payment_value) >= 150 THEN 'Mid Value'
        ELSE 'Low Value'
    END                                           AS spend_segment,
    COUNT(DISTINCT c.customer_unique_id)          AS num_customers,
    ROUND(AVG(p.payment_value), 2)                AS avg_spend,
    ROUND(SUM(p.payment_value), 2)                AS total_revenue
FROM olist_customers c
JOIN olist_orders o          ON c.customer_id = o.customer_id
JOIN olist_order_payments p  ON o.order_id    = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY avg_spend DESC;

-- 5.2 City-level segmentation – top 15 cities by revenue
SELECT
    c.customer_city,
    c.customer_state,
    COUNT(DISTINCT c.customer_unique_id)  AS num_customers,
    ROUND(SUM(p.payment_value), 2)        AS total_revenue,
    ROUND(AVG(p.payment_value), 2)        AS avg_order_value
FROM olist_customers c
JOIN olist_orders o          ON c.customer_id = o.customer_id
JOIN olist_order_payments p  ON o.order_id    = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_city, c.customer_state
ORDER BY total_revenue DESC
LIMIT 15;

-- 5.3 Payment method segmentation
SELECT
    p.payment_type,
    COUNT(DISTINCT c.customer_unique_id)  AS num_customers,
    ROUND(AVG(p.payment_value), 2)        AS avg_spend,
    ROUND(SUM(p.payment_value), 2)        AS total_revenue,
    ROUND(AVG(p.payment_installments), 1) AS avg_installments
FROM olist_customers c
JOIN olist_orders o          ON c.customer_id = o.customer_id
JOIN olist_order_payments p  ON o.order_id    = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY p.payment_type
ORDER BY total_revenue DESC;


-- ============================================================
-- SECTION 6: PRODUCT CATEGORY PREFERENCES BY SEGMENT
-- ============================================================

-- 6.1 Top product categories by revenue
SELECT
    pr.product_category_name,
    COUNT(DISTINCT oi.order_id)           AS total_orders,
    ROUND(SUM(oi.price), 2)               AS total_revenue,
    ROUND(AVG(oi.price), 2)               AS avg_item_price
FROM olist_order_items oi
JOIN olist_products pr ON oi.product_id = pr.product_id
GROUP BY pr.product_category_name
ORDER BY total_revenue DESC
LIMIT 10;

-- 6.2 Category preference by customer state (top 5 states)
SELECT
    c.customer_state,
    pr.product_category_name,
    COUNT(oi.order_id)             AS orders,
    ROUND(SUM(oi.price), 2)        AS revenue
FROM olist_customers c
JOIN olist_orders o      ON c.customer_id  = o.customer_id
JOIN olist_order_items oi ON o.order_id   = oi.order_id
JOIN olist_products pr    ON oi.product_id = pr.product_id
WHERE c.customer_state IN ('SP','RJ','MG','RS','PR')
  AND o.order_status = 'delivered'
GROUP BY c.customer_state, pr.product_category_name
ORDER BY c.customer_state, revenue DESC;

-- 6.3 Review score by product category
SELECT
    pr.product_category_name,
    ROUND(AVG(r.review_score), 2)  AS avg_review,
    COUNT(r.review_id)             AS review_count
FROM olist_order_reviews r
JOIN olist_orders o       ON r.order_id    = o.order_id
JOIN olist_order_items oi ON o.order_id    = oi.order_id
JOIN olist_products pr    ON oi.product_id = pr.product_id
GROUP BY pr.product_category_name
HAVING review_count > 100
ORDER BY avg_review DESC
LIMIT 10;


-- ============================================================
-- SECTION 7: CUSTOMER LOYALTY & RETENTION
-- ============================================================

-- 7.1 One-time vs repeat buyers
SELECT
    CASE
        WHEN order_count = 1 THEN 'One-Time Buyer'
        WHEN order_count BETWEEN 2 AND 3 THEN 'Occasional Buyer'
        ELSE 'Frequent Buyer (4+)'
    END                   AS buyer_type,
    COUNT(*)              AS num_customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_customers
FROM (
    SELECT c.customer_unique_id, COUNT(o.order_id) AS order_count
    FROM olist_customers c
    JOIN olist_orders o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
) sub
GROUP BY buyer_type
ORDER BY num_customers DESC;

-- 7.2 Average days between purchases (repeat buyers only)
SELECT
    c.customer_unique_id,
    COUNT(o.order_id)   AS total_orders,
    MIN(o.order_purchase_timestamp) AS first_purchase,
    MAX(o.order_purchase_timestamp) AS last_purchase,
    DATEDIFF(
        MAX(o.order_purchase_timestamp),
        MIN(o.order_purchase_timestamp)
    )                   AS days_active,
    ROUND(
        DATEDIFF(MAX(o.order_purchase_timestamp), MIN(o.order_purchase_timestamp))
        / NULLIF(COUNT(o.order_id) - 1, 0)
    , 1)                AS avg_days_between_orders
FROM olist_customers c
JOIN olist_orders o ON c.customer_id = o.customer_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_unique_id
HAVING total_orders > 1
ORDER BY avg_days_between_orders ASC
LIMIT 20;

-- 7.3 Monthly new customer acquisition trend
SELECT
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m')  AS month,
    COUNT(DISTINCT c.customer_unique_id)              AS new_customers
FROM olist_customers c
JOIN olist_orders o ON c.customer_id = o.customer_id
WHERE o.order_purchase_timestamp = (
    SELECT MIN(o2.order_purchase_timestamp)
    FROM olist_orders o2
    WHERE o2.customer_id = o.customer_id
)
GROUP BY month
ORDER BY month;


-- ============================================================
-- SECTION 8: ADVANCED QUERIES
-- ============================================================

-- 8.1 Top customers by lifetime value (using subquery)
SELECT
    c.customer_unique_id,
    c.customer_state,
    total_orders,
    total_spend,
    CASE
        WHEN total_spend >= 1000 THEN 'Platinum'
        WHEN total_spend >= 500  THEN 'Gold'
        WHEN total_spend >= 200  THEN 'Silver'
        ELSE 'Bronze'
    END AS loyalty_tier
FROM olist_customers c
JOIN (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id)    AS total_orders,
        ROUND(SUM(p.payment_value), 2) AS total_spend
    FROM olist_orders o
    JOIN olist_order_payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.customer_id
) spend_data ON c.customer_id = spend_data.customer_id
ORDER BY total_spend DESC
LIMIT 30;

-- 8.2 High-value customers who left bad reviews (churn risk)
SELECT
    c.customer_unique_id,
    ROUND(SUM(p.payment_value), 2)  AS total_spend,
    AVG(r.review_score)             AS avg_review,
    COUNT(DISTINCT o.order_id)      AS total_orders
FROM olist_customers c
JOIN olist_orders o          ON c.customer_id = o.customer_id
JOIN olist_order_payments p  ON o.order_id    = p.order_id
JOIN olist_order_reviews r   ON o.order_id    = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_unique_id
HAVING total_spend >= 500 AND avg_review <= 2.5
ORDER BY total_spend DESC;

-- 8.3 Delivery delay impact on review score
SELECT
    CASE
        WHEN DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date) <= 0
            THEN 'On Time / Early'
        WHEN DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date) <= 3
            THEN 'Slightly Late (1–3 days)'
        WHEN DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date) <= 7
            THEN 'Late (4–7 days)'
        ELSE 'Very Late (7+ days)'
    END                              AS delivery_status,
    COUNT(*)                         AS orders,
    ROUND(AVG(r.review_score), 2)    AS avg_review_score
FROM olist_orders o
JOIN olist_order_reviews r ON o.order_id = r.order_id
WHERE o.order_delivered_customer_date IS NOT NULL
GROUP BY delivery_status
ORDER BY avg_review_score DESC;

-- 8.4 Customer segments ranked by revenue contribution (cumulative)
WITH customer_spend AS (
    SELECT
        c.customer_unique_id,
        ROUND(SUM(p.payment_value), 2) AS total_spend
    FROM olist_customers c
    JOIN olist_orders o         ON c.customer_id = o.customer_id
    JOIN olist_order_payments p ON o.order_id    = p.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),
ranked AS (
    SELECT *,
        ROUND(SUM(total_spend) OVER (ORDER BY total_spend DESC) /
              SUM(total_spend) OVER () * 100, 2) AS cumulative_pct
    FROM customer_spend
)
SELECT *,
    CASE
        WHEN cumulative_pct <= 20 THEN 'Top 20% – High Value'
        WHEN cumulative_pct <= 50 THEN 'Mid Tier'
        ELSE 'Long Tail'
    END AS revenue_band
FROM ranked
ORDER BY total_spend DESC
LIMIT 50;

-- ============================================================
-- END OF SCRIPT
-- ============================================================
