-- ============================================================================
-- TABLE_LINEAGE_MAP: Single source of truth for table name mappings
-- Used by: KG SP, KG visualization, DDL validation
-- ============================================================================

CREATE TABLE IF NOT EXISTS DBAONTAP_ANALYTICS.METADATA.TABLE_LINEAGE_MAP (
    SOURCE_SCHEMA VARCHAR NOT NULL,
    SOURCE_TABLE VARCHAR NOT NULL,
    TARGET_SCHEMA VARCHAR NOT NULL,
    TARGET_TABLE VARCHAR NOT NULL,
    EDGE_TYPE VARCHAR NOT NULL DEFAULT 'TRANSFORMS_TO',
    RELATIONSHIP_LABEL VARCHAR DEFAULT 'transform',
    DESCRIPTION VARCHAR,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_lineage PRIMARY KEY (SOURCE_SCHEMA, SOURCE_TABLE, TARGET_SCHEMA, TARGET_TABLE)
);

TRUNCATE TABLE DBAONTAP_ANALYTICS.METADATA.TABLE_LINEAGE_MAP;

INSERT INTO DBAONTAP_ANALYTICS.METADATA.TABLE_LINEAGE_MAP 
    (SOURCE_SCHEMA, SOURCE_TABLE, TARGET_SCHEMA, TARGET_TABLE, EDGE_TYPE, RELATIONSHIP_LABEL, DESCRIPTION)
VALUES
    -- Bronze -> Silver (TRANSFORMS_TO)
    ('BRONZE', 'CUSTOMERS_VARIANT',       'SILVER', 'CUSTOMERS',        'TRANSFORMS_TO', 'transform', 'Bronze VARIANT to typed Silver'),
    ('BRONZE', 'ORDERS_VARIANT',          'SILVER', 'ORDERS',           'TRANSFORMS_TO', 'transform', 'Bronze VARIANT to typed Silver'),
    ('BRONZE', 'ORDER_ITEMS_VARIANT',     'SILVER', 'ORDER_ITEMS',      'TRANSFORMS_TO', 'transform', 'Bronze VARIANT to typed Silver'),
    ('BRONZE', 'PRODUCTS_VARIANT',        'SILVER', 'PRODUCTS_VARIANT', 'TRANSFORMS_TO', 'transform', 'Bronze VARIANT to typed Silver'),
    ('BRONZE', 'SUPPORT_TICKETS_VARIANT', 'SILVER', 'SUPPORT_TICKETS',  'TRANSFORMS_TO', 'transform', 'Bronze VARIANT to typed Silver'),
    -- Silver -> Gold (AGGREGATES_TO)
    ('SILVER', 'CUSTOMERS',        'GOLD', 'CUSTOMER_360',               'AGGREGATES_TO', 'aggregate', 'Customer dimension to 360 view'),
    ('SILVER', 'ORDERS',           'GOLD', 'CUSTOMER_360',               'AGGREGATES_TO', 'join',      'Order history joined to 360 view'),
    ('SILVER', 'SUPPORT_TICKETS',  'GOLD', 'CUSTOMER_360',               'AGGREGATES_TO', 'join',      'Support history joined to 360 view'),
    ('SILVER', 'CUSTOMERS',        'GOLD', 'ML_CUSTOMER_FEATURES',       'AGGREGATES_TO', 'features',  'Customer features for ML'),
    ('SILVER', 'ORDERS',           'GOLD', 'ML_CUSTOMER_FEATURES',       'AGGREGATES_TO', 'features',  'Order features for ML'),
    ('SILVER', 'ORDERS',           'GOLD', 'ORDER_SUMMARY',              'AGGREGATES_TO', 'aggregate', 'Orders aggregated to summary'),
    ('SILVER', 'CUSTOMERS',        'GOLD', 'ORDER_SUMMARY',              'AGGREGATES_TO', 'join',      'Customer info joined to order summary'),
    ('SILVER', 'ORDER_ITEMS',      'GOLD', 'ORDER_SUMMARY',              'AGGREGATES_TO', 'join',      'Line items joined to order summary'),
    ('SILVER', 'PRODUCTS_VARIANT', 'GOLD', 'PRODUCT_PERFORMANCE_METRICS','AGGREGATES_TO', 'aggregate', 'Product data aggregated to performance'),
    ('SILVER', 'ORDER_ITEMS',      'GOLD', 'PRODUCT_PERFORMANCE_METRICS','AGGREGATES_TO', 'join',      'Order items joined to product performance'),
    ('SILVER', 'SUPPORT_TICKETS',  'GOLD', 'SUPPORT_METRICS',            'AGGREGATES_TO', 'aggregate', 'Tickets aggregated to support metrics');
