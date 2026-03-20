"""
Agentic Silver Layer Demo Manager
=================================
Streamlit app to manage and demo the CDC pipeline:
- Reset/delete data across all layers
- Generate sample data in SOURCE PostgreSQL
- Insert CDC records into LANDING PostgreSQL
- View transformation logs and errors
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta
import random
import string

# ============================================================================
# Configuration
# ============================================================================
st.set_page_config(
    page_title="Agentic Silver Layer Demo",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Connection settings from environment or defaults
SNOWFLAKE_CONNECTION = os.getenv("SNOWFLAKE_CONNECTION_NAME", "CoCo-Green")

# PostgreSQL hosts (from .pgpass)
SOURCE_PG_HOST = os.getenv("SOURCE_PG_HOST", "source-pg-host.example.snowflake.app")
LANDING_PG_HOST = os.getenv("LANDING_PG_HOST", "landing-pg-host.example.snowflake.app")
PG_USER = "snowflake_admin"
PG_DATABASE = "dbaontap"

# ============================================================================
# Database Connections
# ============================================================================
def load_snowflake_config(connection_name: str) -> dict:
    """Load connection config from ~/.snowflake/connections.toml"""
    import tomllib
    config_path = os.path.expanduser("~/.snowflake/connections.toml")
    with open(config_path, 'rb') as f:
        config = tomllib.load(f)
    return config.get(connection_name, {})

@st.cache_resource
def get_snowflake_connection():
    """Get Snowflake connection using connector with explicit key loading"""
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    
    cfg = load_snowflake_config(SNOWFLAKE_CONNECTION)
    
    with open(os.path.expanduser(cfg.get('private_key_path', '')), 'rb') as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,
            backend=default_backend()
        )
    
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    return snowflake.connector.connect(
        account=cfg.get('account'),
        user=cfg.get('user'),
        private_key=pkb,
        role=cfg.get('role'),
        warehouse=cfg.get('warehouse'),
        database='DBAONTAP_ANALYTICS'
    )

def get_postgres_connection(host: str):
    """Get PostgreSQL connection"""
    import psycopg2
    # Read password from .pgpass
    pgpass_path = os.path.expanduser("~/.pgpass")
    password = None
    if os.path.exists(pgpass_path):
        with open(pgpass_path, 'r') as f:
            for line in f:
                parts = line.strip().split(':')
                if len(parts) >= 5 and host.startswith(parts[0].split('.')[0]):
                    password = parts[4]
                    break
    
    if not password:
        st.error(f"Password not found in ~/.pgpass for host: {host}")
        return None
    
    return psycopg2.connect(
        host=host,
        port=5432,
        database=PG_DATABASE,
        user=PG_USER,
        password=password
    )

# ============================================================================
# Helper Functions
# ============================================================================
def run_snowflake_query(query: str, fetch: bool = True):
    """Execute Snowflake query and return results"""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        if fetch:
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns) if columns else pd.DataFrame()
        return True
    except Exception as e:
        st.error(f"Snowflake error: {e}")
        return None

def run_postgres_query(host: str, query: str, fetch: bool = True):
    """Execute PostgreSQL query and return results"""
    try:
        conn = get_postgres_connection(host)
        if not conn:
            return None
        cursor = conn.cursor()
        cursor.execute(query)
        if fetch:
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            data = cursor.fetchall()
            conn.commit()
            return pd.DataFrame(data, columns=columns) if columns else pd.DataFrame()
        conn.commit()
        return True
    except Exception as e:
        st.error(f"PostgreSQL error ({host[:20]}...): {e}")
        return None

def generate_random_customer():
    """Generate random customer data"""
    first_names = ["John", "Sarah", "Mike", "Emily", "David", "Lisa", "James", "Anna", "Robert", "Jennifer", 
                   "William", "Emma", "Michael", "Olivia", "Daniel", "Sophia", "Matthew", "Isabella", "Andrew", "Mia"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
                  "Wilson", "Anderson", "Taylor", "Thomas", "Moore", "Jackson", "Martin", "Lee", "Thompson", "White"]
    companies = ["Acme Corp", "GlobalTech", "Enterprise Inc", "Startup Labs", "BigCorp", "Innovate Co", 
                 "Solutions Ltd", "TechStart", "DataFlow", "CloudNine", "FutureTech", "NextGen Systems"]
    industries = ["Technology", "Software", "Finance", "Healthcare", "Manufacturing", "Retail", "Consulting", "Analytics"]
    
    first = random.choice(first_names)
    last = random.choice(last_names)
    email = f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(['gmail.com', 'company.com', 'corp.io'])}"
    
    return {
        "first_name": first,
        "last_name": last,
        "email": email,
        "phone": f"555-{random.randint(1000,9999)}",
        "company_name": random.choice(companies),
        "industry": random.choice(industries)
    }

def generate_random_order(customer_id: int):
    """Generate random order data"""
    statuses = ["pending", "processing", "shipped", "completed"]
    return {
        "customer_id": customer_id,
        "status": random.choice(statuses),
        "total_amount": round(random.uniform(1000, 100000), 2),
        "shipping_address": f"{random.randint(100,9999)} {random.choice(['Main', 'Oak', 'Pine', 'Maple'])} St"
    }

# ============================================================================
# UI Components
# ============================================================================
def render_sidebar():
    """Render sidebar with navigation and status"""
    st.sidebar.title("🔄 Demo Manager")
    st.sidebar.markdown("---")
    
    # Connection status
    st.sidebar.subheader("Connection Status")
    
    # Check Snowflake
    try:
        get_snowflake_connection()
        st.sidebar.success("✅ Snowflake")
    except:
        st.sidebar.error("❌ Snowflake")
    
    # Check PostgreSQL connections
    for name, host in [("SOURCE PG", SOURCE_PG_HOST), ("LANDING PG", LANDING_PG_HOST)]:
        try:
            conn = get_postgres_connection(host)
            if conn:
                conn.close()
                st.sidebar.success(f"✅ {name}")
            else:
                st.sidebar.error(f"❌ {name}")
        except:
            st.sidebar.error(f"❌ {name}")
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("Quick Links")
    st.sidebar.markdown("""
    - [Snowsight](https://app.snowflake.com)
    - [Documentation](./docs/architecture.md)
    """)

def render_delete_tab():
    """Render data deletion interface"""
    st.header("🗑️ Delete Table Data")
    st.markdown("Reset data across all layers for a clean demo.")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("SOURCE PostgreSQL")
        if st.button("🔴 Delete SOURCE Data", key="del_source", type="secondary"):
            with st.spinner("Deleting SOURCE data..."):
                tables = ["order_items", "orders", "support_tickets", "products", "customers"]
                for table in tables:
                    result = run_postgres_query(SOURCE_PG_HOST, f"DELETE FROM {table}", fetch=False)
                    if result:
                        st.success(f"✓ Deleted {table}")
                    else:
                        st.error(f"✗ Failed {table}")
    
    with col2:
        st.subheader("LANDING PostgreSQL")
        if st.button("🔴 Delete LANDING Data", key="del_landing", type="secondary"):
            with st.spinner("Deleting LANDING data..."):
                tables = ["order_items", "orders", "support_tickets", "products", "customers"]
                for table in tables:
                    result = run_postgres_query(LANDING_PG_HOST, f"DELETE FROM {table}", fetch=False)
                    if result:
                        st.success(f"✓ Deleted {table}")
                    else:
                        st.error(f"✗ Failed {table}")
    
    with col3:
        st.subheader("Snowflake (Openflow)")
        st.warning("⚠️ Openflow tables are managed by CDC")
        if st.button("🔴 Truncate Snowflake Tables", key="del_sf", type="secondary"):
            with st.spinner("Truncating Snowflake tables..."):
                tables = ["customers", "orders", "products", "order_items", "support_tickets"]
                for table in tables:
                    result = run_snowflake_query(
                        f'DELETE FROM DBAONTAP_ANALYTICS."public"."{table}"', 
                        fetch=False
                    )
                    if result:
                        st.success(f"✓ Deleted {table}")
    
    st.markdown("---")
    
    # Delete transformation logs
    st.subheader("🧹 Clear Metadata")
    if st.button("Clear Transformation Logs", key="clear_logs"):
        result = run_snowflake_query(
            "DELETE FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG",
            fetch=False
        )
        if result:
            st.success("Transformation logs cleared")

def render_source_data_tab():
    """Render SOURCE data generation interface"""
    st.header("📊 Generate SOURCE Data")
    st.markdown("Generate sample data in SOURCE PostgreSQL to demo migration to LANDING.")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Data Generation")
        num_customers = st.slider("Number of Customers", 5, 100, 20)
        num_orders = st.slider("Orders per Customer (max)", 1, 10, 3)
        num_products = st.slider("Number of Products", 3, 20, 6)
        
        if st.button("🚀 Generate Data", type="primary"):
            with st.spinner("Generating data..."):
                progress = st.progress(0)
                
                # Generate products first
                st.write("Creating products...")
                products = [
                    ("Enterprise License", "Software", 50000, 5000, f"ENT-{i:03d}"),
                    ("Professional License", "Software", 15000, 1500, f"PRO-{i:03d}"),
                    ("Starter License", "Software", 5000, 500, f"STR-{i:03d}"),
                    ("Premium Support", "Services", 25000, 10000, f"SUP-{i:03d}"),
                    ("Standard Support", "Services", 10000, 4000, f"STD-{i:03d}"),
                    ("Training Package", "Services", 7500, 3000, f"TRN-{i:03d}"),
                ]
                for i, (name, cat, price, cost, sku) in enumerate(products[:num_products]):
                    sku = f"{sku.split('-')[0]}-{random.randint(100,999)}"
                    run_postgres_query(SOURCE_PG_HOST, f"""
                        INSERT INTO products (product_name, category, price, cost, sku)
                        VALUES ('{name}', '{cat}', {price}, {cost}, '{sku}')
                        ON CONFLICT (sku) DO NOTHING
                    """, fetch=False)
                progress.progress(20)
                
                # Generate customers
                st.write("Creating customers...")
                customer_ids = []
                for i in range(num_customers):
                    c = generate_random_customer()
                    result = run_postgres_query(SOURCE_PG_HOST, f"""
                        INSERT INTO customers (first_name, last_name, email, phone, company_name, industry)
                        VALUES ('{c['first_name']}', '{c['last_name']}', '{c['email']}', '{c['phone']}', '{c['company_name']}', '{c['industry']}')
                        ON CONFLICT (email) DO NOTHING
                        RETURNING customer_id
                    """)
                    if result is not None and len(result) > 0:
                        customer_ids.append(result.iloc[0, 0])
                    progress.progress(20 + int(40 * (i + 1) / num_customers))
                
                # Generate orders
                st.write("Creating orders...")
                for i, cid in enumerate(customer_ids):
                    for _ in range(random.randint(1, num_orders)):
                        o = generate_random_order(cid)
                        run_postgres_query(SOURCE_PG_HOST, f"""
                            INSERT INTO orders (customer_id, status, total_amount, shipping_address)
                            VALUES ({o['customer_id']}, '{o['status']}', {o['total_amount']}, '{o['shipping_address']}')
                        """, fetch=False)
                    progress.progress(60 + int(40 * (i + 1) / len(customer_ids)))
                
                progress.progress(100)
                st.success(f"✅ Generated {num_customers} customers, {num_products} products, and orders!")
    
    with col2:
        st.subheader("Current SOURCE Data")
        if st.button("🔄 Refresh Counts"):
            pass
        
        counts = run_postgres_query(SOURCE_PG_HOST, """
            SELECT 'customers' as table_name, COUNT(*) as count FROM customers
            UNION ALL SELECT 'products', COUNT(*) FROM products
            UNION ALL SELECT 'orders', COUNT(*) FROM orders
            UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
            UNION ALL SELECT 'support_tickets', COUNT(*) FROM support_tickets
        """)
        if counts is not None:
            st.dataframe(counts, use_container_width=True)

def render_cdc_tab():
    """Render CDC demo interface for LANDING"""
    st.header("🔄 CDC Demo - Insert to LANDING")
    st.markdown("Insert records directly into LANDING PostgreSQL to demo Openflow CDC to Snowflake.")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("Quick Insert")
        
        insert_type = st.selectbox("Record Type", ["Customer", "Order", "Support Ticket"])
        num_records = st.number_input("Number of Records", 1, 50, 5)
        
        if st.button("➕ Insert Records", type="primary"):
            with st.spinner(f"Inserting {num_records} {insert_type}(s)..."):
                if insert_type == "Customer":
                    max_id_result = run_postgres_query(LANDING_PG_HOST, "SELECT COALESCE(MAX(customer_id), 0) as m FROM customers")
                    max_id = int(max_id_result.iloc[0, 0]) if max_id_result is not None else 0
                    for i in range(num_records):
                        c = generate_random_customer()
                        segment = random.choice(["Enterprise", "SMB", "Startup", "Mid-Market"])
                        annual_revenue = round(random.uniform(50000, 5000000), 2)
                        customer_id = max_id + i + 1
                        run_postgres_query(LANDING_PG_HOST, f"""
                            INSERT INTO customers (customer_id, email, first_name, last_name, company_name, phone, segment, annual_revenue, is_active, created_at, updated_at)
                            VALUES ({customer_id}, '{c['email']}', '{c['first_name']}', '{c['last_name']}', '{c['company_name']}', '{c['phone']}', '{segment}', {annual_revenue}, true, NOW(), NOW())
                            ON CONFLICT (email) DO NOTHING
                        """, fetch=False)
                    st.success(f"✅ Inserted {num_records} customers into LANDING")
                
                elif insert_type == "Order":
                    # Get existing customer IDs
                    customers = run_postgres_query(LANDING_PG_HOST, "SELECT customer_id FROM customers LIMIT 100")
                    if customers is not None and len(customers) > 0:
                        for i in range(num_records):
                            cid = random.choice(customers['customer_id'].tolist())
                            o = generate_random_order(cid)
                            run_postgres_query(LANDING_PG_HOST, f"""
                                INSERT INTO orders (customer_id, status, total_amount, shipping_address)
                                VALUES ({o['customer_id']}, '{o['status']}', {o['total_amount']}, '{o['shipping_address']}')
                            """, fetch=False)
                        st.success(f"✅ Inserted {num_records} orders into LANDING")
                    else:
                        st.warning("No customers found. Insert customers first.")
                
                elif insert_type == "Support Ticket":
                    customers = run_postgres_query(LANDING_PG_HOST, "SELECT customer_id FROM customers LIMIT 100")
                    if customers is not None and len(customers) > 0:
                        subjects = ["Login issue", "Billing question", "Feature request", "Performance problem", "Integration help"]
                        priorities = ["low", "medium", "high"]
                        for i in range(num_records):
                            cid = random.choice(customers['customer_id'].tolist())
                            run_postgres_query(LANDING_PG_HOST, f"""
                                INSERT INTO support_tickets (customer_id, subject, description, priority, status)
                                VALUES ({cid}, '{random.choice(subjects)}', 'Demo ticket description', '{random.choice(priorities)}', 'open')
                            """, fetch=False)
                        st.success(f"✅ Inserted {num_records} support tickets into LANDING")
                    else:
                        st.warning("No customers found. Insert customers first.")
    
    with col2:
        st.subheader("Pipeline Status")
        
        # Show counts across pipeline
        st.markdown("**Record Counts by Layer:**")
        
        pipeline_data = []
        
        # LANDING counts
        landing_counts = run_postgres_query(LANDING_PG_HOST, """
            SELECT 'customers' as tbl, COUNT(*) as cnt FROM customers
            UNION ALL SELECT 'orders', COUNT(*) FROM orders
            UNION ALL SELECT 'products', COUNT(*) FROM products
        """)
        
        # Snowflake counts
        sf_counts = run_snowflake_query("""
            SELECT 'customers' as tbl, COUNT(*) as cnt FROM DBAONTAP_ANALYTICS."public"."customers"
            UNION ALL SELECT 'orders', COUNT(*) FROM DBAONTAP_ANALYTICS."public"."orders"
            UNION ALL SELECT 'products', COUNT(*) FROM DBAONTAP_ANALYTICS."public"."products"
        """)
        
        bronze_counts = run_snowflake_query("""
            SELECT 'customers' as tbl, COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT
            UNION ALL SELECT 'orders', COUNT(*) FROM DBAONTAP_ANALYTICS.BRONZE.ORDERS_VARIANT
            UNION ALL SELECT 'products', COUNT(*) FROM DBAONTAP_ANALYTICS.BRONZE.PRODUCTS_VARIANT
        """)
        
        silver_counts = run_snowflake_query("""
            SELECT 'customers' as tbl, COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.SILVER.CUSTOMERS
            UNION ALL SELECT 'orders', COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.ORDERS
            UNION ALL SELECT 'products', COUNT(*) FROM DBAONTAP_ANALYTICS.SILVER.PRODUCTS
        """)
        
        gold_counts = run_snowflake_query("""
            SELECT 'CUSTOMER_360' as tbl, COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360
        """)
        
        # Build comparison table - normalize column names to lowercase
        def normalize_df(df):
            if df is not None:
                df.columns = df.columns.str.lower()
            return df
        
        landing_counts = normalize_df(landing_counts)
        sf_counts = normalize_df(sf_counts)
        bronze_counts = normalize_df(bronze_counts)
        silver_counts = normalize_df(silver_counts)
        
        if all([landing_counts is not None, sf_counts is not None]):
            comparison = pd.DataFrame({
                'Table': ['customers', 'orders', 'products'],
                'LANDING': landing_counts.set_index('tbl')['cnt'].reindex(['customers', 'orders', 'products']).fillna(0).astype(int).tolist() if landing_counts is not None else [0,0,0],
                'Snowflake': sf_counts.set_index('tbl')['cnt'].reindex(['customers', 'orders', 'products']).fillna(0).astype(int).tolist() if sf_counts is not None else [0,0,0],
                'BRONZE': bronze_counts.set_index('tbl')['cnt'].reindex(['customers', 'orders', 'products']).fillna(0).astype(int).tolist() if bronze_counts is not None else [0,0,0],
                'SILVER': silver_counts.set_index('tbl')['cnt'].reindex(['customers', 'orders', 'products']).fillna(0).astype(int).tolist() if silver_counts is not None else [0,0,0],
            })
            st.dataframe(comparison, use_container_width=True)
        
        if st.button("🔄 Refresh Pipeline Status"):
            st.rerun()

def render_logs_tab():
    """Render transformation logs and errors"""
    st.header("📋 Transformation Logs & Errors")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Filter options
        status_filter = st.multiselect(
            "Filter by Status",
            ["SUCCESS", "FAILED", "PENDING"],
            default=["FAILED", "SUCCESS"]
        )
    
    with col2:
        limit = st.number_input("Max Records", 10, 500, 100)
    
    # Query transformation logs
    status_list = "'" + "','".join(status_filter) + "'"
    logs = run_snowflake_query(f"""
        SELECT 
            transformation_id,
            source_table,
            target_table,
            status,
            agent_reasoning,
            executed_at,
            transformation_sql
        FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG
        WHERE status IN ({status_list})
        ORDER BY executed_at DESC
        LIMIT {limit}
    """)
    
    if logs is not None and len(logs) > 0:
        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            success_count = len(logs[logs['STATUS'] == 'SUCCESS'])
            st.metric("Successful", success_count, delta=None)
        with col2:
            failed_count = len(logs[logs['STATUS'] == 'FAILED'])
            st.metric("Failed", failed_count, delta=None, delta_color="inverse")
        with col3:
            pending_count = len(logs[logs['STATUS'] == 'PENDING'])
            st.metric("Pending", pending_count)
        
        st.markdown("---")
        
        # Show logs
        st.subheader("Transformation Log")
        
        # Display without SQL column first
        display_cols = ['SOURCE_TABLE', 'TARGET_TABLE', 'STATUS', 'AGENT_REASONING', 'EXECUTED_AT']
        st.dataframe(
            logs[display_cols].style.applymap(
                lambda x: 'background-color: #ffcccc' if x == 'FAILED' else ('background-color: #ccffcc' if x == 'SUCCESS' else ''),
                subset=['STATUS']
            ),
            use_container_width=True
        )
        
        # Show failed items with details
        failed_logs = logs[logs['STATUS'] == 'FAILED']
        if len(failed_logs) > 0:
            st.markdown("---")
            st.subheader("❌ Failed Transformations (Manual Fix Required)")
            
            for idx, row in failed_logs.iterrows():
                with st.expander(f"🔴 {row['TARGET_TABLE']} - {row['EXECUTED_AT']}"):
                    st.markdown(f"**Source:** {row['SOURCE_TABLE']}")
                    st.markdown(f"**Error:** {row['AGENT_REASONING']}")
                    st.markdown("**Attempted SQL:**")
                    st.code(row['TRANSFORMATION_SQL'], language="sql")
    else:
        st.info("No transformation logs found. Run the semantic view pipeline first.")
    
    st.markdown("---")
    
    # Semantic Views Status
    st.subheader("🔷 Semantic Views Status")
    sv_list = run_snowflake_query("""
        SELECT name, created_on, owner
        FROM INFORMATION_SCHEMA.SEMANTIC_VIEWS
        WHERE TABLE_SCHEMA = 'GOLD'
        ORDER BY created_on DESC
    """)
    
    if sv_list is not None and len(sv_list) > 0:
        st.success(f"✅ {len(sv_list)} Semantic Views Created")
        st.dataframe(sv_list, use_container_width=True)
    else:
        st.warning("No semantic views found. Run the pipeline to generate them.")
    
    # Re-run pipeline button
    st.markdown("---")
    if st.button("🔄 Re-run Semantic View Pipeline", type="primary"):
        with st.spinner("Running agentic semantic view pipeline..."):
            result = run_snowflake_query(
                "CALL DBAONTAP_ANALYTICS.AGENTS.RUN_SEMANTIC_VIEW_PIPELINE()"
            )
            if result is not None:
                st.json(result.iloc[0, 0] if len(result) > 0 else {})
                st.success("Pipeline completed! Refresh to see results.")

# ============================================================================
# Main App
# ============================================================================
def main():
    render_sidebar()
    
    st.title("🔄 Agentic Silver Layer Demo Manager")
    st.markdown("Manage and demonstrate the CDC pipeline from PostgreSQL to Snowflake Intelligence.")
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🗑️ Delete Data",
        "📊 Generate SOURCE Data", 
        "🔄 CDC Demo (LANDING)",
        "📋 Logs & Errors"
    ])
    
    with tab1:
        render_delete_tab()
    
    with tab2:
        render_source_data_tab()
    
    with tab3:
        render_cdc_tab()
    
    with tab4:
        render_logs_tab()

if __name__ == "__main__":
    main()
