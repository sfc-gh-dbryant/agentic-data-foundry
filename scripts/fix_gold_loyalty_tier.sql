-- Add loyalty_tier to Gold CUSTOMER_360 DT
CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360
TARGET_LAG = '1 hour'
WAREHOUSE = DBRYANT_COCO_WH_S
AS
SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    c.email,
    c.company_name,
    c.segment,
    c.loyalty_tier,
    CASE
        WHEN c.annual_revenue >= 1000000 THEN 'ENTERPRISE'
        WHEN c.annual_revenue >= 100000 THEN 'MID-MARKET'
        ELSE 'SMB'
    END AS revenue_tier,
    c.annual_revenue,
    c.is_active,
    c.created_at as customer_since,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_amount) as lifetime_value,
    AVG(o.total_amount) as avg_order_value,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) as days_since_last_order,
    DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) as recency_days,
    COUNT(DISTINCT o.order_id) as frequency_orders,
    SUM(o.total_amount) as monetary_total,
    COUNT(DISTINCT t.ticket_id) as total_tickets,
    AVG(DATEDIFF('hour', t.created_at, t.resolved_at)) as avg_resolution_hours,
    CASE
        WHEN DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) > 90 THEN 'AT_RISK'
        WHEN DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) > 60 THEN 'COOLING'
        WHEN DATEDIFF('day', MAX(o.order_date), CURRENT_DATE()) > 30 THEN 'ACTIVE'
        ELSE 'HOT'
    END as engagement_status,
    c.updated_at as last_updated
FROM DBAONTAP_ANALYTICS.SILVER.CUSTOMERS c
LEFT JOIN DBAONTAP_ANALYTICS.SILVER.ORDERS o ON c.customer_id = o.customer_id
LEFT JOIN DBAONTAP_ANALYTICS.SILVER.SUPPORT_TICKETS t ON c.customer_id = t.customer_id
WHERE c.is_active = TRUE
GROUP BY
    c.customer_id, c.first_name, c.last_name, c.email, c.company_name,
    c.segment, c.loyalty_tier, c.annual_revenue, c.is_active, c.created_at, c.updated_at;
