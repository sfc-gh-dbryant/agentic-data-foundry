"""
Agentic DE Demo Manager (with PostgreSQL EAI)
==============================================
Streamlit in Snowflake app with External Access Integration to PostgreSQL.


Features:
- Generate synthetic data directly to PostgreSQL SOURCE
- Replicate data to LANDING (demo logical replication)
- Insert CDC records to LANDING (demo Openflow CDC)
- View pipeline status across all layers
- Run agentic semantic view pipeline
- View transformation logs and errors
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
from datetime import datetime, timedelta
import random
import json

# PostgreSQL connectivity via EAI
try:
    import psycopg2
    from psycopg2.extras import execute_values
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

# ============================================================================
# Initialize Session
# ============================================================================
session = get_active_session()

# PostgreSQL connection config (via EAI secrets)
PG_SOURCE_HOST = "source-pg-host.example.snowflake.app"
PG_LANDING_HOST = "landing-pg-host.example.snowflake.app"
PG_DATABASE = "dbaontap"
PG_PORT = 5432

# ============================================================================
# Page Configuration
# ============================================================================
st.set_page_config(
    page_title="Agentic DE Demo",
    page_icon="🔄",
    layout="wide"
)

# ============================================================================
# Sample Data Generators
# ============================================================================
FIRST_NAMES = ["John", "Sarah", "Mike", "Emily", "David", "Lisa", "James", "Anna", "Robert", "Jennifer", 
               "William", "Emma", "Michael", "Olivia", "Daniel", "Sophia", "Matthew", "Isabella", "Andrew", "Mia",
               "Christopher", "Ava", "Joshua", "Charlotte", "Ethan", "Amelia", "Alexander", "Harper", "Ryan", "Evelyn"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
              "Wilson", "Anderson", "Taylor", "Thomas", "Moore", "Jackson", "Martin", "Lee", "Thompson", "White",
              "Harris", "Clark", "Lewis", "Robinson", "Walker", "Young", "King", "Wright", "Hill", "Scott"]

COMPANIES = ["Acme Corp", "GlobalTech", "Enterprise Inc", "Startup Labs", "BigCorp", "Innovate Co", 
             "Solutions Ltd", "TechStart", "DataFlow", "CloudNine", "FutureTech", "NextGen Systems",
             "Digital Dynamics", "Smart Solutions", "Peak Performance", "Velocity Partners", "Quantum Analytics"]

INDUSTRIES = ["Technology", "Software", "Finance", "Healthcare", "Manufacturing", "Retail", "Consulting", 
              "Analytics", "Cloud Services", "E-commerce", "Logistics", "Media", "Education", "Energy"]

PRODUCTS = [
    {"name": "Enterprise License", "category": "Software", "price": 50000, "cost": 5000},
    {"name": "Professional License", "category": "Software", "price": 15000, "cost": 1500},
    {"name": "Starter License", "category": "Software", "price": 5000, "cost": 500},
    {"name": "Premium Support", "category": "Services", "price": 25000, "cost": 10000},
    {"name": "Standard Support", "category": "Services", "price": 10000, "cost": 4000},
    {"name": "Training Package", "category": "Services", "price": 7500, "cost": 3000},
    {"name": "Consulting Hours", "category": "Services", "price": 20000, "cost": 8000},
    {"name": "Data Integration", "category": "Software", "price": 35000, "cost": 7000},
    {"name": "API Access", "category": "Software", "price": 12000, "cost": 2000},
    {"name": "Analytics Module", "category": "Software", "price": 18000, "cost": 3500},
]

ORDER_STATUSES = ["pending", "processing", "shipped", "completed", "cancelled"]
TICKET_PRIORITIES = ["low", "medium", "high", "critical"]
TICKET_STATUSES = ["open", "in_progress", "resolved", "closed"]
TICKET_SUBJECTS = [
    "Login issue", "Billing question", "Feature request", "Performance problem", 
    "Integration help", "Account access", "License activation", "Data export",
    "API error", "Configuration help", "Upgrade inquiry", "Security concern"
]

def generate_email(first, last, idx):
    domains = ["gmail.com", "company.com", "corp.io", "business.net", "enterprise.org"]
    return f"{first.lower()}.{last.lower()}{idx}@{random.choice(domains)}"

def generate_phone():
    return f"555-{random.randint(1000, 9999)}"

def generate_address():
    streets = ["Main", "Oak", "Pine", "Maple", "Cedar", "Elm", "Park", "Lake", "River", "Hill"]
    return f"{random.randint(100, 9999)} {random.choice(streets)} St, City, ST {random.randint(10000, 99999)}"

# ============================================================================
# PostgreSQL Connection (via EAI)
# ============================================================================
def get_pg_connection(target: str = "source"):
    """Get PostgreSQL connection using EAI secrets"""
    if not POSTGRES_AVAILABLE:
        return None
    
    try:
        # Get credentials from Snowflake secrets
        if target == "source":
            secret_name = "pg_source_creds"
            host = PG_SOURCE_HOST
        else:
            secret_name = "pg_landing_creds"
            host = PG_LANDING_HOST
        
        # Access secret via _snowflake module (SiS runtime)
        import _snowflake
        creds = _snowflake.get_username_password(secret_name)
        
        conn = psycopg2.connect(
            host=host,
            port=PG_PORT,
            database=PG_DATABASE,
            user=creds.username,
            password=creds.password,
            sslmode='require'
        )
        return conn
    except Exception as e:
        st.error(f"PostgreSQL connection failed: {e}")
        return None

def pg_execute(sql: str, params=None, target: str = "source", fetch: bool = False):
    """Execute PostgreSQL query via EAI"""
    conn = get_pg_connection(target)
    if not conn:
        return None
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetch:
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return pd.DataFrame(rows, columns=columns)
            conn.commit()
            return True
    except Exception as e:
        st.error(f"PostgreSQL query error: {e}")
        return None
    finally:
        conn.close()

def pg_insert_many(table: str, columns: list, data: list, target: str = "source"):
    """Bulk insert to PostgreSQL"""
    if not data:
        return 0
    
    conn = get_pg_connection(target)
    if not conn:
        return 0
    
    try:
        with conn.cursor() as cur:
            placeholders = ', '.join(['%s'] * len(columns))
            col_names = ', '.join(columns)
            sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
            
            values = [[row.get(c) for c in columns] for row in data]
            cur.executemany(sql, values)
            conn.commit()
            return len(data)
    except Exception as e:
        st.error(f"PostgreSQL insert error: {e}")
        return 0
    finally:
        conn.close()

# ============================================================================
# Snowflake Operations
# ============================================================================
def sf_query(sql: str, fetch: bool = True):
    """Execute Snowflake query"""
    try:
        result = session.sql(sql)
        if fetch:
            return result.to_pandas()
        else:
            result.collect()
            return True
    except Exception as e:
        st.error(f"Snowflake query error: {e}")
        return None

def get_table_counts():
    """Get row counts across all pipeline layers"""
    counts = {}
    
    # Landing (Snowflake public schema - synced from PG LANDING via Openflow)
    for table in ["customers", "orders", "products", "order_items", "support_tickets"]:
        try:
            result = session.sql(f'SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS."public"."{table}"').collect()
            counts[f"landing_{table}"] = result[0]['CNT']
        except:
            counts[f"landing_{table}"] = 0
    
    # Bronze
    for table in ["CUSTOMERS_VARIANT", "ORDERS_VARIANT", "PRODUCTS_VARIANT", "ORDER_ITEMS_VARIANT", "SUPPORT_TICKETS_VARIANT"]:
        try:
            result = session.sql(f'SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.BRONZE.{table}').collect()
            counts[f"bronze_{table.lower()}"] = result[0]['CNT']
        except:
            counts[f"bronze_{table.lower()}"] = 0
    
    # Silver
    for table in ["CUSTOMERS", "ORDERS", "PRODUCTS", "ORDER_ITEMS", "SUPPORT_TICKETS"]:
        try:
            result = session.sql(f'SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.SILVER.{table}').collect()
            counts[f"silver_{table.lower()}"] = result[0]['CNT']
        except:
            counts[f"silver_{table.lower()}"] = 0
    
    # Gold
    for table in ["CUSTOMER_360", "PRODUCT_PERFORMANCE", "ORDER_SUMMARY", "CUSTOMER_METRICS", "ML_CUSTOMER_FEATURES"]:
        try:
            result = session.sql(f'SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.GOLD.{table}').collect()
            counts[f"gold_{table.lower()}"] = result[0]['CNT']
        except:
            counts[f"gold_{table.lower()}"] = 0
    
    return counts

# ============================================================================
# Data Generation
# ============================================================================
def generate_customers(num_rows: int, start_id: int = 1):
    customers = []
    for i in range(num_rows):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        customers.append({
            "customer_id": start_id + i,
            "first_name": first,
            "last_name": last,
            "email": generate_email(first, last, start_id + i),
            "phone": generate_phone(),
            "company_name": random.choice(COMPANIES),
            "segment": random.choice(["STANDARD", "PREMIUM", "ENTERPRISE"]),
            "annual_revenue": round(random.uniform(50000, 5000000), 2),
            "is_active": random.choice([True, True, True, False]),
            "created_at": datetime.now() - timedelta(days=random.randint(1, 365)),
            "updated_at": datetime.now()
        })
    return customers

def generate_products(num_rows: int, start_id: int = 1):
    products = []
    for i in range(num_rows):
        base = random.choice(PRODUCTS)
        products.append({
            "product_id": start_id + i,
            "sku": f"{base['category'][:3].upper()}-{start_id + i:04d}",
            "name": base["name"],
            "description": f"High-quality {base['name'].lower()} for enterprise customers",
            "category": base["category"],
            "list_price": round(base["price"] * random.uniform(0.8, 1.2), 2),
            "cost_price": round(base["cost"] * random.uniform(0.8, 1.2), 2),
            "is_active": random.choice([True, True, True, False]),
            "created_at": datetime.now() - timedelta(days=random.randint(1, 365))
        })
    return products

def generate_orders(num_rows: int, customer_ids: list, start_id: int = 1):
    orders = []
    for i in range(num_rows):
        order_date = datetime.now() - timedelta(days=random.randint(1, 90))
        orders.append({
            "order_id": start_id + i,
            "customer_id": random.choice(customer_ids),
            "order_date": order_date.date(),
            "status": random.choice(ORDER_STATUSES),
            "total_amount": round(random.uniform(1000, 100000), 2),
            "payment_method": random.choice(["credit_card", "wire_transfer", "invoice", "ach"]),
            "notes": None,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })
    return orders

def generate_order_items(num_rows: int, order_ids: list, product_ids: list, start_id: int = 1):
    items = []
    for i in range(num_rows):
        qty = random.randint(1, 10)
        price = round(random.uniform(500, 50000), 2)
        items.append({
            "order_item_id": start_id + i,
            "order_id": random.choice(order_ids),
            "product_id": random.choice(product_ids),
            "quantity": qty,
            "unit_price": price,
            "line_total": round(qty * price, 2),
            "created_at": datetime.now()
        })
    return items

def generate_support_tickets(num_rows: int, customer_ids: list, start_id: int = 1):
    tickets = []
    for i in range(num_rows):
        created = datetime.now() - timedelta(days=random.randint(1, 60))
        status = random.choice(TICKET_STATUSES)
        resolved = created + timedelta(hours=random.randint(1, 72)) if status in ["resolved", "closed"] else None
        tickets.append({
            "ticket_id": start_id + i,
            "customer_id": random.choice(customer_ids),
            "subject": random.choice(TICKET_SUBJECTS),
            "description": f"Demo ticket description - ID {start_id + i}",
            "priority": random.choice(TICKET_PRIORITIES),
            "status": status,
            "created_at": created,
            "resolved_at": resolved
        })
    return tickets

# ============================================================================
# UI Components
# ============================================================================
def render_sidebar():
    st.sidebar.title("🔄 Pipeline Status")
    
    if st.sidebar.button("🔄 Refresh"):
        st.experimental_rerun()
    
    st.sidebar.markdown("---")
    
    counts = get_table_counts()
    st.sidebar.metric("Landing", counts.get("landing_customers", 0))
    st.sidebar.metric("Silver", counts.get("silver_customers", 0))
    st.sidebar.metric("Gold", counts.get("gold_customer_360", 0))
    
    st.sidebar.markdown("---")
    
    # EAI status
    if POSTGRES_AVAILABLE:
        st.sidebar.success("✅ PostgreSQL EAI Ready")
    else:
        st.sidebar.warning("⚠️ PostgreSQL unavailable")
    
    # Semantic views count
    try:
        sv_count = session.sql("""
            SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.SEMANTIC_VIEWS 
            WHERE SCHEMA = 'GOLD'
        """).collect()[0]['CNT']
        st.sidebar.metric("Semantic Views", sv_count)
    except:
        st.sidebar.metric("Semantic Views", 0)

def render_generate_tab():
    st.header("📊 Generate Synthetic Data")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        st.subheader("Target Database")
        
        if POSTGRES_AVAILABLE:
            target = st.radio(
                "Insert data to:",
                ["PostgreSQL SOURCE", "PostgreSQL LANDING"],
                help="""
                **SOURCE**: Demo initial migration (SOURCE → LANDING via logical replication)
                **LANDING**: Demo CDC flow (LANDING → Snowflake via Openflow)
                """
            )
        else:
            st.error("PostgreSQL EAI not available. Cannot generate data.")
            return
        
        st.markdown("---")
        
        st.subheader("Row Counts")
        num_customers = st.number_input("Customers", 1, 1000, 50, 10)
        num_products = st.number_input("Products", 1, 100, 10, 5)
        num_orders = st.number_input("Orders", 1, 5000, 100, 50)
        num_order_items = st.number_input("Order Items", 1, 10000, 200, 50)
        num_tickets = st.number_input("Support Tickets", 0, 500, 25, 10)
        
        st.markdown("---")
        
        generate_btn = st.button("🚀 Generate Data", type="primary", use_container_width=True)
    
    with col2:
        st.subheader("Data Flow")
        if "SOURCE" in target:
            st.info("📤 Data → PostgreSQL SOURCE → (Logical Replication) → PostgreSQL LANDING → (Openflow CDC) → Snowflake")
        else:
            st.info("📤 Data → PostgreSQL LANDING → (Openflow CDC) → Snowflake")
        
        if generate_btn:
            with st.spinner("Generating data..."):
                progress = st.progress(0, "Starting...")
                results = {}
                
                db_target = "source" if "SOURCE" in target else "landing"
                
                # Get max IDs from target PostgreSQL
                try:
                    max_cust = pg_execute("SELECT COALESCE(MAX(customer_id), 0) FROM customers", target=db_target, fetch=True).iloc[0, 0]
                    max_prod = pg_execute("SELECT COALESCE(MAX(product_id), 0) FROM products", target=db_target, fetch=True).iloc[0, 0]
                    max_order = pg_execute("SELECT COALESCE(MAX(order_id), 0) FROM orders", target=db_target, fetch=True).iloc[0, 0]
                    max_item = pg_execute("SELECT COALESCE(MAX(order_item_id), 0) FROM order_items", target=db_target, fetch=True).iloc[0, 0]
                    max_ticket = pg_execute("SELECT COALESCE(MAX(ticket_id), 0) FROM support_tickets", target=db_target, fetch=True).iloc[0, 0]
                except Exception as e:
                    st.error(f"Failed to query PostgreSQL: {e}")
                    return
                
                # Generate data
                progress.progress(10, "Generating customers...")
                customers = generate_customers(num_customers, int(max_cust) + 1)
                customer_ids = [c["customer_id"] for c in customers]
                
                progress.progress(20, "Generating products...")
                products = generate_products(num_products, int(max_prod) + 1)
                product_ids = [p["product_id"] for p in products]
                
                progress.progress(30, "Generating orders...")
                orders = generate_orders(num_orders, customer_ids, int(max_order) + 1) if customer_ids else []
                order_ids = [o["order_id"] for o in orders]
                
                progress.progress(40, "Generating order items...")
                items = generate_order_items(num_order_items, order_ids, product_ids, int(max_item) + 1) if order_ids and product_ids else []
                
                progress.progress(50, "Generating tickets...")
                tickets = generate_support_tickets(num_tickets, customer_ids, int(max_ticket) + 1) if customer_ids and num_tickets > 0 else []
                
                # Insert to PostgreSQL
                progress.progress(60, f"Inserting to PostgreSQL {db_target.upper()}...")
                results['customers'] = pg_insert_many("customers", 
                    ["customer_id", "first_name", "last_name", "email", "phone", "company_name", "segment", "annual_revenue", "is_active", "created_at", "updated_at"],
                    customers, target=db_target)
                results['products'] = pg_insert_many("products",
                    ["product_id", "sku", "name", "description", "category", "list_price", "cost_price", "is_active", "created_at"],
                    products, target=db_target)
                results['orders'] = pg_insert_many("orders",
                    ["order_id", "customer_id", "order_date", "status", "total_amount", "payment_method", "notes", "created_at", "updated_at"],
                    orders, target=db_target)
                results['order_items'] = pg_insert_many("order_items",
                    ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_total", "created_at"],
                    items, target=db_target)
                results['support_tickets'] = pg_insert_many("support_tickets",
                    ["ticket_id", "customer_id", "subject", "description", "priority", "status", "created_at", "resolved_at"],
                    tickets, target=db_target)
                
                progress.progress(100, "Complete!")
                
                st.success(f"✅ Data inserted to PostgreSQL {db_target.upper()}!")
                st.json(results)
                
                if db_target == "landing":
                    st.info("💡 Openflow CDC will sync this data to Snowflake within ~1 minute.")
                else:
                    st.info("💡 Logical replication will sync to LANDING, then Openflow CDC to Snowflake.")
        else:
            # Show counts from selected PostgreSQL target
            db_target = "source" if "SOURCE" in target else "landing"
            st.markdown(f"**Current PostgreSQL {db_target.upper()} Counts**")
            
            pg_counts = pg_execute("""
                SELECT 'customers' as table_name, COUNT(*) as count FROM customers
                UNION ALL SELECT 'products', COUNT(*) FROM products
                UNION ALL SELECT 'orders', COUNT(*) FROM orders
                UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
                UNION ALL SELECT 'support_tickets', COUNT(*) FROM support_tickets
            """, target=db_target, fetch=True)
            
            if pg_counts is not None:
                st.dataframe(pg_counts, use_container_width=True)
            
            st.markdown("---")
            st.markdown("**Snowflake Landing Counts** (via Openflow CDC)")
            sf_counts = sf_query("""
                SELECT 'customers' as table_name, COUNT(*) as count FROM DBAONTAP_ANALYTICS."public"."customers"
                UNION ALL SELECT 'products', COUNT(*) FROM DBAONTAP_ANALYTICS."public"."products"
                UNION ALL SELECT 'orders', COUNT(*) FROM DBAONTAP_ANALYTICS."public"."orders"
                UNION ALL SELECT 'order_items', COUNT(*) FROM DBAONTAP_ANALYTICS."public"."order_items"
                UNION ALL SELECT 'support_tickets', COUNT(*) FROM DBAONTAP_ANALYTICS."public"."support_tickets"
            """)
            if sf_counts is not None:
                st.dataframe(sf_counts, use_container_width=True)

def render_pipeline_tab():
    st.header("🔄 Pipeline Status")
    
    st.markdown("""
    ```
    PG SOURCE → PG LANDING → SF Landing → Bronze (VARIANT) → Silver (CDC-aware) → Gold → Semantic Views
              ↑           ↑              ↑
         Logical      Openflow        Dynamic
         Replication   CDC            Tables
    ```
    """)
    
    counts = get_table_counts()
    
    # Get PostgreSQL SOURCE counts
    source_counts = {}
    if POSTGRES_AVAILABLE:
        try:
            for table in ["customers", "orders", "products", "order_items", "support_tickets"]:
                result = pg_execute(f"SELECT COUNT(*) as cnt FROM {table}", target="source", fetch=True)
                if result is not None:
                    source_counts[table] = result.iloc[0, 0]
        except:
            pass
    
    # Create tile-style cards
    col0, col1, col2, col3, col4 = st.columns(5)
    
    tile_style = "padding: 20px; border-radius: 10px; text-align: center; height: 250px; display: flex; flex-direction: column; justify-content: flex-start;"
    
    with col0:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #336791 0%, #2d5a7b 100%); color: white; {}">
            <h4 style="margin: 0 0 10px 0; color: white;">🐘 PG Source</h4>
            <p style="margin: 5px 0; font-size: 14px;">Customers: <b>{}</b></p>
            <p style="margin: 5px 0; font-size: 14px;">Orders: <b>{}</b></p>
            <p style="margin: 5px 0; font-size: 14px;">Products: <b>{}</b></p>
        </div>
        """.format(
            tile_style,
            source_counts.get("customers", 0),
            source_counts.get("orders", 0),
            source_counts.get("products", 0)
        ), unsafe_allow_html=True)
    
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #29B5E8 0%, #1a8bb8 100%); color: white; {}">
            <h4 style="margin: 0 0 10px 0; color: white;">📥 SF Landing</h4>
            <p style="margin: 5px 0; font-size: 14px;">Customers: <b>{}</b></p>
            <p style="margin: 5px 0; font-size: 14px;">Orders: <b>{}</b></p>
            <p style="margin: 5px 0; font-size: 14px;">Products: <b>{}</b></p>
        </div>
        """.format(
            tile_style,
            counts.get("landing_customers", 0),
            counts.get("landing_orders", 0),
            counts.get("landing_products", 0)
        ), unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #CD7F32 0%, #a66628 100%); color: white; {}">
            <h4 style="margin: 0 0 10px 0; color: white;">🥉 Bronze</h4>
            <p style="margin: 5px 0; font-size: 14px;">Customers: <b>{}</b></p>
            <p style="margin: 5px 0; font-size: 14px;">Orders: <b>{}</b></p>
            <p style="margin: 5px 0; font-size: 14px;">Products: <b>{}</b></p>
        </div>
        """.format(
            tile_style,
            counts.get("bronze_customers_variant", 0),
            counts.get("bronze_orders_variant", 0),
            counts.get("bronze_products_variant", 0)
        ), unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #C0C0C0 0%, #8a8a8a 100%); color: #333; {}">
            <h4 style="margin: 0 0 10px 0; color: #333;">🥈 Silver</h4>
            <p style="margin: 5px 0; font-size: 14px;">Customers: <b>{}</b></p>
            <p style="margin: 5px 0; font-size: 14px;">Orders: <b>{}</b></p>
            <p style="margin: 5px 0; font-size: 14px;">Products: <b>{}</b></p>
        </div>
        """.format(
            tile_style,
            counts.get("silver_customers", 0),
            counts.get("silver_orders", 0),
            counts.get("silver_products", 0)
        ), unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #FFD700 0%, #d4af37 100%); color: #333; {}">
            <h4 style="margin: 0 0 10px 0; color: #333;">🥇 Gold</h4>
            <p style="margin: 5px 0; font-size: 14px;">Customer 360: <b>{}</b></p>
            <p style="margin: 5px 0; font-size: 14px;">Product Perf: <b>{}</b></p>
            <p style="margin: 5px 0; font-size: 14px;">Order Sum: <b>{}</b></p>
        </div>
        """.format(
            tile_style,
            counts.get("gold_customer_360", 0),
            counts.get("gold_product_performance", 0),
            counts.get("gold_order_summary", 0)
        ), unsafe_allow_html=True)

def render_semantic_tab():
    st.header("🔷 Semantic Views (Agentic Pipeline)")
    
    # Current Semantic Views - Full Width on Top
    st.subheader("Current Semantic Views")
    sv_list = sf_query("""
        SELECT name as "Name", created as "Created", owner as "Owner"
        FROM INFORMATION_SCHEMA.SEMANTIC_VIEWS
        WHERE SCHEMA = 'GOLD' ORDER BY created DESC
    """)
    if sv_list is not None and len(sv_list) > 0:
        st.dataframe(sv_list, use_container_width=True)
    else:
        st.warning("No semantic views found. Run the pipeline below to generate them.")
    
    st.markdown("---")
    
    # Generate Semantic Views Section
    st.subheader("🤖 Generate Semantic Views")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        The agentic pipeline uses **Cortex LLM (claude-3-7-sonnet)** to:
        1. **Discover** all Gold tables automatically
        2. **Analyze** schema structure and infer primary keys
        3. **Generate** semantic view DDL with dimensions, facts, and metrics
        4. **Execute** the DDL and log results (success/failure)
        """)
    
    with col2:
        if st.button("🚀 Run Pipeline", type="primary", use_container_width=True):
            with st.spinner("Running agentic pipeline... (this may take a minute)"):
                result = sf_query("CALL DBAONTAP_ANALYTICS.AGENTS.RUN_SEMANTIC_VIEW_PIPELINE()")
                if result is not None and len(result) > 0:
                    result_json = result.iloc[0, 0]
                    if isinstance(result_json, str):
                        result_json = json.loads(result_json)
                    st.success(f"✅ {result_json.get('success_count', 0)} succeeded, {result_json.get('fail_count', 0)} failed")
                    with st.expander("View Details"):
                        details = result_json.get('details', [])
                        for item in details:
                            status = item.get('status', 'UNKNOWN')
                            table = item.get('table', 'Unknown')
                            view = item.get('view', 'Unknown')
                            retries = item.get('retries', 0)
                            
                            if status == 'SUCCESS':
                                icon = "✅"
                                retry_text = f" (auto-corrected after {retries} {'retry' if retries == 1 else 'retries'})" if retries > 0 else ""
                                st.markdown(f"{icon} **{table}** → `{view}`{retry_text}")
                            else:
                                icon = "❌"
                                error = item.get('error', 'Unknown error')
                                st.markdown(f"{icon} **{table}** → `{view}` - Failed after {retries} attempts")
                                st.code(error, language=None)

def render_logs_tab():
    st.header("📋 Logs & Errors")
    
    status_filter = st.multiselect("Status", ["SUCCESS", "FAILED", "PENDING"], default=["FAILED", "SUCCESS"])
    
    if status_filter:
        status_list = "'" + "','".join(status_filter) + "'"
        logs = sf_query(f"""
            SELECT source_table, target_table, status, agent_reasoning, executed_at
            FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG
            WHERE status IN ({status_list})
            ORDER BY executed_at DESC LIMIT 100
        """)
        
        if logs is not None and len(logs) > 0:
            st.dataframe(logs, use_container_width=True)
            
            failed = logs[logs['STATUS'] == 'FAILED']
            if len(failed) > 0:
                st.subheader("❌ Failed Items")
                for _, row in failed.iterrows():
                    with st.expander(f"🔴 {row['TARGET_TABLE']}"):
                        st.write(f"**Error:** {row['AGENT_REASONING']}")

def render_reset_tab():
    st.header("🗑️ Reset Data")
    st.warning("⚠️ Caution: These actions delete data!")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Delete Snowflake Landing Data")
        tables = st.multiselect("Tables", ["customers", "orders", "products", "order_items", "support_tickets"])
        
        if st.button("🗑️ Delete Selected"):
            if tables:
                ordered = []
                if "order_items" in tables: ordered.append("order_items")
                if "orders" in tables: ordered.append("orders")
                if "support_tickets" in tables: ordered.append("support_tickets")
                if "products" in tables: ordered.append("products")
                if "customers" in tables: ordered.append("customers")
                
                for t in ordered:
                    sf_query(f'DELETE FROM DBAONTAP_ANALYTICS."public"."{t}"', fetch=False)
                    st.success(f"✓ Deleted {t}")
    
    with col2:
        st.subheader("Clear Logs")
        if st.button("Clear Transformation Logs"):
            sf_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG", fetch=False)
            st.success("Cleared!")

# ============================================================================
# Main
# ============================================================================
def call_cortex_analyst(sv_fqn: str, question: str, chat_history: list = None):
    """Call Cortex Analyst REST API via _snowflake module"""
    import _snowflake
    
    # Build messages array for multi-turn conversation
    messages = []
    if chat_history:
        for msg in chat_history:
            if msg["role"] == "user":
                messages.append({
                    "role": "user",
                    "content": [{"type": "text", "text": msg["content"]}]
                })
            elif msg["role"] == "assistant" and "sql" in msg:
                messages.append({
                    "role": "analyst",
                    "content": [
                        {"type": "text", "text": msg["content"]},
                        {"type": "sql", "statement": msg["sql"]}
                    ]
                })
    
    # Add current question
    messages.append({
        "role": "user",
        "content": [{"type": "text", "text": question}]
    })
    
    # Build request body
    request_body = {
        "messages": messages,
        "semantic_view": sv_fqn
    }
    
    # Call the REST API
    resp = _snowflake.send_snow_api_request(
        "POST",
        "/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000  # 30 second timeout
    )
    
    if resp["status"] == 200:
        return json.loads(resp["content"])
    else:
        raise Exception(f"API error {resp['status']}: {resp.get('content', 'Unknown error')}")

def render_chatbot_tab():
    st.header("💬 AI Assistant (Cortex Analyst)")
    
    # Get available semantic views
    sv_list = sf_query("""
        SELECT name FROM INFORMATION_SCHEMA.SEMANTIC_VIEWS
        WHERE SCHEMA = 'GOLD' ORDER BY name
    """)
    
    if sv_list is None or len(sv_list) == 0:
        st.warning("No semantic views available. Run the Agentic Pipeline first to create them.")
        return
    
    sv_names = sv_list['NAME'].tolist()
    
    # Auto-select best semantic view (prefer CUSTOMER_360 variants)
    default_sv = None
    for preferred in ["CUSTOMER_360_AGENTIC_SV", "CUSTOMER_360_SV", "CUSTOMER_ANALYTICS_SV"]:
        if preferred in sv_names:
            default_sv = preferred
            break
    if not default_sv:
        default_sv = sv_names[0]
    
    # Store selected SV in session state
    if "selected_sv" not in st.session_state:
        st.session_state.selected_sv = default_sv
    
    # Show advanced options in expander
    with st.expander("⚙️ Advanced Options"):
        selected_sv = st.selectbox(
            "Semantic View", 
            sv_names, 
            index=sv_names.index(st.session_state.selected_sv),
            help="Select which semantic view to query. Default is auto-selected."
        )
        st.session_state.selected_sv = selected_sv
    
    st.caption(f"Using: **{st.session_state.selected_sv}**")
    
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Display chat history
    for message in st.session_state.messages:
        role_icon = "🧑" if message["role"] == "user" else "🤖"
        st.markdown(f"**{role_icon} {message['role'].title()}:** {message['content']}")
        if "sql" in message:
            with st.expander("View SQL"):
                st.code(message["sql"], language="sql")
        if "data" in message:
            st.dataframe(message["data"])
        st.markdown("---")
    
    # Chat input (compatible with older Streamlit)
    col1, col2 = st.columns([5, 1])
    with col1:
        prompt = st.text_input("Ask a question about your data...", key="chat_input", label_visibility="collapsed")
    with col2:
        send_btn = st.button("Send", type="primary", use_container_width=True)
    
    if send_btn and prompt:
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Call Cortex Analyst with self-correction loop
        sv_fqn = f"DBAONTAP_ANALYTICS.GOLD.{st.session_state.selected_sv}"
        max_retries = 3
        current_prompt = prompt
        retry_history = []
        
        for attempt in range(max_retries):
            with st.spinner(f"{'Thinking...' if attempt == 0 else f'Retrying (attempt {attempt + 1}/{max_retries})...'}"):
                try:
                    # Build conversation context for retry
                    if attempt > 0:
                        # Feed error back to Cortex Analyst
                        current_prompt = f"""The previous SQL query failed with this error:
```
{last_error}
```

Original question: {prompt}

Please fix the SQL query to resolve this error."""
                    
                    # Call Cortex Analyst REST API
                    response = call_cortex_analyst(sv_fqn, current_prompt, 
                        st.session_state.messages[:-1] + retry_history if attempt > 0 else st.session_state.messages[:-1])
                    
                    # Parse response
                    message_text = ""
                    sql = ""
                    
                    if "message" in response:
                        content = response.get("message", {}).get("content", [])
                        for item in content:
                            if item.get("type") == "text":
                                message_text = item.get("text", "")
                            elif item.get("type") == "sql":
                                sql = item.get("statement", "")
                    
                    if sql:
                        # Show attempt info
                        if attempt > 0:
                            st.info(f"🔄 Auto-corrected (attempt {attempt + 1})")
                        
                        with st.expander("View Generated SQL"):
                            st.code(sql, language="sql")
                        
                        # Execute the generated SQL
                        try:
                            data = sf_query(sql)
                            if data is not None and len(data) > 0:
                                if message_text:
                                    st.markdown(message_text)
                                st.dataframe(data, use_container_width=True)
                                st.session_state.messages.append({
                                    "role": "assistant",
                                    "content": message_text or "Query executed successfully.",
                                    "sql": sql,
                                    "data": data,
                                    "retries": attempt
                                })
                                break  # Success! Exit retry loop
                            else:
                                if message_text:
                                    st.markdown(message_text)
                                st.info("Query returned no results.")
                                st.session_state.messages.append({
                                    "role": "assistant",
                                    "content": message_text + "\n\n*Query returned no results.*",
                                    "sql": sql,
                                    "retries": attempt
                                })
                                break  # No error, just empty results
                                
                        except Exception as exec_err:
                            last_error = str(exec_err)
                            # Add failed attempt to retry history for context
                            retry_history.append({
                                "role": "assistant",
                                "content": f"SQL execution failed: {last_error}",
                                "sql": sql
                            })
                            
                            if attempt == max_retries - 1:
                                # Final attempt failed
                                st.error(f"❌ Failed after {max_retries} attempts. Last error: {last_error}")
                                with st.expander("View Failed SQL"):
                                    st.code(sql, language="sql")
                                st.session_state.messages.append({
                                    "role": "assistant",
                                    "content": f"Failed after {max_retries} attempts. Last error: {last_error}",
                                    "sql": sql,
                                    "error": last_error
                                })
                            else:
                                st.warning(f"⚠️ Attempt {attempt + 1} failed: {last_error[:100]}... Retrying...")
                    else:
                        # No SQL generated
                        if message_text:
                            st.markdown(message_text)
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": message_text or "I couldn't generate SQL for that query."
                        })
                        break
                        
                except Exception as e:
                    if attempt == max_retries - 1:
                        error_msg = f"Error: {str(e)}"
                        st.error(error_msg)
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": error_msg
                        })
                    else:
                        last_error = str(e)
                        st.warning(f"⚠️ API error, retrying...")
    
    # Clear chat button
    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.experimental_rerun()

def main():
    render_sidebar()
    
    st.title("🔄 Agentic DE Demo")
    st.caption("CDC Pipeline: PostgreSQL → Snowflake Intelligence")
    
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Generate Data", "🔄 Pipeline", "🔷 Semantic Views", "💬 AI Chat", "📋 Logs", "🗑️ Reset"
    ])
    
    with tab1:
        render_generate_tab()
    with tab2:
        render_pipeline_tab()
    with tab3:
        render_semantic_tab()
    with tab4:
        render_chatbot_tab()
    with tab5:
        render_logs_tab()
    with tab6:
        render_reset_tab()

if __name__ == "__main__":
    main()
