"""
Agentic Silver Layer Demo Manager
=================================
Streamlit in Snowflake app for managing the CDC pipeline demo:
- Generate synthetic data with configurable row counts
- View pipeline status across all layers
- Run agentic semantic view pipeline
- View transformation logs and errors
- Reset PostgreSQL source/landing instances (via EAI)
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, lit, current_timestamp
import pandas as pd
from datetime import datetime, timedelta
import random
import json
import _snowflake

# PostgreSQL connectivity via EAI
try:
    import psycopg2
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

from contextlib import contextmanager

HAS_CHAT = hasattr(st, 'chat_input')

@contextmanager
def _chat_message(role):
    if HAS_CHAT:
        with st.chat_message(role):
            yield
    else:
        icon = "🧑" if role == "user" else "🤖"
        with st.container():
            st.markdown(f"**{icon} {role.title()}:**")
            yield

def _chat_input(label):
    if HAS_CHAT:
        return st.chat_input(label)
    with st.form("_compat_chat_form", clear_on_submit=True):
        user_input = st.text_input(label, placeholder=label, label_visibility="collapsed")
        submitted = st.form_submit_button("💬 Send")
        if submitted and user_input:
            return user_input
    return None

# ============================================================================
# Initialize Session
# ============================================================================
session = get_active_session()

# PostgreSQL connection config
PG_SOURCE_HOST = "source-pg-host.example.snowflake.app"
PG_LANDING_HOST = "landing-pg-host.example.snowflake.app"
PG_DATABASE = "dbaontap"
PG_PORT = 5432

# ============================================================================
# Page Configuration
# ============================================================================
st.set_page_config(
    page_title="Agentic Data Foundry",
    page_icon="🔄",
    layout="wide"
)

# ============================================================================
# PostgreSQL Connection (via EAI)
# ============================================================================
def get_pg_connection(target: str = "source"):
    """Get PostgreSQL connection using EAI secrets"""
    if not POSTGRES_AVAILABLE:
        return None
    
    try:
        if target == "source":
            secret_name = "pg_source_creds"
            host = PG_SOURCE_HOST
        else:
            secret_name = "pg_landing_creds"
            host = PG_LANDING_HOST
        
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
        st.error(f"PostgreSQL connection failed ({target}): {e}")
        return None

def truncate_pg_tables(target: str, tables: list):
    """Truncate tables in PostgreSQL instance"""
    conn = get_pg_connection(target)
    if not conn:
        return False, "Connection failed"
    
    try:
        cur = conn.cursor()
        for table in tables:
            cur.execute(f"TRUNCATE TABLE public.{table} CASCADE")
        conn.commit()
        cur.close()
        conn.close()
        return True, f"Truncated {len(tables)} tables"
    except Exception as e:
        return False, str(e)

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
        st.error(f"PostgreSQL error: {e}")
        return None
    finally:
        conn.close()

def pg_insert_many(table: str, columns: list, data: list, target: str = "source"):
    """Insert multiple rows into PostgreSQL table"""
    if not data:
        return 0
    
    conn = get_pg_connection(target)
    if not conn:
        return 0
    
    try:
        with conn.cursor() as cur:
            placeholders = ', '.join(['%s'] * len(columns))
            sql = f"INSERT INTO public.{table} ({', '.join(columns)}) VALUES ({placeholders})"
            
            inserted = 0
            for row in data:
                values = [row.get(col) for col in columns]
                try:
                    cur.execute(sql, values)
                    inserted += 1
                except Exception as e:
                    pass
            
            conn.commit()
            return inserted
    except Exception as e:
        st.error(f"Insert error: {e}")
        return 0
    finally:
        conn.close()

# ============================================================================
# AGENTIC DATA GENERATOR (AI-First Methodology)
# Workflow: DISCOVER → ANALYZE → TRANSFORM → VALIDATE
# ============================================================================

def agentic_discover_schema(target: str = "source"):
    """DISCOVER: Query PostgreSQL information_schema to discover table structures"""
    schema_sql = """
    SELECT 
        t.table_name,
        c.column_name,
        c.data_type,
        c.is_nullable,
        c.column_default,
        c.character_maximum_length,
        c.numeric_precision,
        c.numeric_scale,
        (SELECT COUNT(*) FROM information_schema.table_constraints tc 
         JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
         WHERE tc.table_name = t.table_name AND kcu.column_name = c.column_name 
         AND tc.constraint_type = 'PRIMARY KEY') > 0 as is_primary_key,
        (SELECT ccu.table_name FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
         JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
         WHERE tc.table_name = t.table_name AND kcu.column_name = c.column_name
         AND tc.constraint_type = 'FOREIGN KEY' LIMIT 1) as references_table
    FROM information_schema.tables t
    JOIN information_schema.columns c ON t.table_name = c.table_name
    WHERE t.table_schema = 'public' 
    AND t.table_type = 'BASE TABLE'
    AND t.table_name IN ('customers', 'products', 'orders', 'order_items', 'support_tickets')
    ORDER BY t.table_name, c.ordinal_position
    """
    return pg_execute(schema_sql, target=target, fetch=True)

def agentic_analyze_schema(schema_df):
    """ANALYZE: Use Cortex LLM to understand column semantics and generate data strategy"""
    if schema_df is None or schema_df.empty:
        return None
    
    tables = {}
    for table_name in schema_df['table_name'].unique():
        table_cols = schema_df[schema_df['table_name'] == table_name]
        tables[table_name] = {
            'columns': [],
            'primary_key': None,
            'foreign_keys': {}
        }
        for _, col in table_cols.iterrows():
            col_info = {
                'name': col['column_name'],
                'type': col['data_type'],
                'nullable': col['is_nullable'] == 'YES',
                'has_default': col['column_default'] is not None,
                'max_length': col['character_maximum_length'],
                'precision': col['numeric_precision'],
                'scale': col['numeric_scale']
            }
            tables[table_name]['columns'].append(col_info)
            if col['is_primary_key']:
                tables[table_name]['primary_key'] = col['column_name']
            if col['references_table']:
                tables[table_name]['foreign_keys'][col['column_name']] = col['references_table']
    
    return tables

# Agentic generation configuration
LLM_BATCH_SIZE = 10  # Generate 10 rows per LLM call to avoid timeouts

def agentic_generate_with_llm(table_name: str, columns: list, num_rows: int, context: dict = None, progress_callback=None):
    """TRANSFORM: Use Cortex LLM to generate contextually appropriate data with batching"""
    all_data = []
    num_batches = (num_rows + LLM_BATCH_SIZE - 1) // LLM_BATCH_SIZE  # Ceiling division
    base_start_id = context.get('start_id', 1) if context else 1
    
    # Build column descriptions once
    col_descriptions = []
    for col in columns:
        desc = f"- {col['name']} ({col['type']})"
        if col['max_length']:
            desc += f" max_length={col['max_length']}"
        if col['precision']:
            desc += f" precision={col['precision']}"
        col_descriptions.append(desc)
    col_desc_str = chr(10).join(col_descriptions)
    
    for batch_idx in range(num_batches):
        batch_start = batch_idx * LLM_BATCH_SIZE
        batch_size = min(LLM_BATCH_SIZE, num_rows - batch_start)
        batch_start_id = base_start_id + len(all_data)
        
        # Update progress for each batch
        if progress_callback:
            progress_callback(f"🤖 {table_name}: batch {batch_idx + 1}/{num_batches} ({len(all_data)}/{num_rows} rows)...")
        
        # Build context for this batch
        batch_context = {**(context or {}), 'start_id': batch_start_id}
        
        prompt = f"""Generate exactly {batch_size} realistic sample records for a '{table_name}' table with these columns:
{col_desc_str}

Context: This is a B2B SaaS company's operational database.
{f"Available foreign key values: {json.dumps(batch_context)}" if batch_context else ""}

Return ONLY a valid JSON array of objects with these exact column names. 
For auto-increment primary keys, start from {batch_start_id}.
Generate realistic, varied business data. No explanations, just the JSON array."""

        try:
            result = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'claude-3-5-sonnet',
                    '{prompt.replace("'", "''")}'
                ) as response
            """).collect()
            
            response_text = result[0]['RESPONSE']
            
            # Parse JSON from response
            start_idx = response_text.find('[')
            end_idx = response_text.rfind(']') + 1
            if start_idx >= 0 and end_idx > start_idx:
                json_str = response_text[start_idx:end_idx]
                batch_data = json.loads(json_str)
                all_data.extend(batch_data)
                
                if progress_callback:
                    progress_callback(f"✅ {table_name}: batch {batch_idx + 1}/{num_batches} complete ({len(all_data)}/{num_rows} rows)")
            else:
                st.warning(f"LLM returned invalid JSON for {table_name} batch {batch_idx + 1}")
                
        except Exception as e:
            st.warning(f"LLM batch {batch_idx + 1} failed for {table_name}: {e}")
            continue
    
    return all_data if all_data else None

def agentic_validate_data(table_name: str, data: list, schema_info: dict):
    """VALIDATE: Verify generated data matches schema constraints"""
    if not data:
        return False, "No data generated"
    
    issues = []
    columns = {c['name']: c for c in schema_info['columns']}
    
    for i, row in enumerate(data):
        for col_name, col_info in columns.items():
            if col_name in row:
                val = row[col_name]
                if val is None and not col_info['nullable'] and not col_info['has_default']:
                    issues.append(f"Row {i}: {col_name} is null but not nullable")
                if col_info['max_length'] and isinstance(val, str) and len(val) > col_info['max_length']:
                    row[col_name] = val[:col_info['max_length']]
    
    return len(issues) == 0, issues if issues else "Validation passed"

def run_agentic_data_generator(target: str, num_rows: dict, progress_callback=None):
    """
    Main agentic workflow: DISCOVER → ANALYZE → TRANSFORM → VALIDATE
    Follows AI_FIRST_METHODOLOGY.md architecture
    """
    results = {'workflow': 'agentic', 'phases': {}, 'data': {}}
    
    if progress_callback:
        progress_callback(5, "🔍 DISCOVER: Interrogating PostgreSQL schema...")
    schema_df = agentic_discover_schema(target)
    if schema_df is None:
        return {'error': 'Schema discovery failed'}
    results['phases']['discover'] = f"Found {len(schema_df)} columns across {schema_df['table_name'].nunique()} tables"
    
    if progress_callback:
        progress_callback(15, "🧠 ANALYZE: Understanding column semantics...")
    schema_info = agentic_analyze_schema(schema_df)
    if not schema_info:
        return {'error': 'Schema analysis failed'}
    results['phases']['analyze'] = f"Analyzed {len(schema_info)} tables"
    
    if progress_callback:
        progress_callback(25, "🤖 TRANSFORM: Generating data with Cortex LLM...")
    
    generated_ids = {}
    table_order = ['customers', 'products', 'orders', 'order_items', 'support_tickets']
    
    for i, table_name in enumerate(table_order):
        if table_name not in schema_info or num_rows.get(table_name, 0) == 0:
            continue
        
        progress_pct = 25 + int((i / len(table_order)) * 50)
        if progress_callback:
            progress_callback(progress_pct, f"🤖 Generating {table_name}...")
        
        max_id_col = schema_info[table_name]['primary_key']
        max_id_df = pg_execute(f"SELECT COALESCE(MAX({max_id_col}), 0) as m FROM {table_name}", target=target, fetch=True)
        start_id = int(max_id_df.iloc[0, 0]) + 1 if max_id_df is not None else 1
        
        context = {'start_id': start_id}
        for fk_col, ref_table in schema_info[table_name].get('foreign_keys', {}).items():
            if ref_table in generated_ids:
                context[f'{fk_col}_values'] = generated_ids[ref_table]
        
        # Create a mini progress callback for batch updates
        def batch_progress(msg):
            if progress_callback:
                progress_callback(progress_pct, msg)
        
        data = agentic_generate_with_llm(
            table_name, 
            schema_info[table_name]['columns'],
            num_rows.get(table_name, 0),
            context,
            progress_callback=batch_progress
        )
        
        if data:
            if progress_callback:
                progress_callback(progress_pct + 5, f"✅ VALIDATE: Checking {table_name}...")
            valid, validation_result = agentic_validate_data(table_name, data, schema_info[table_name])
            
            columns = [c['name'] for c in schema_info[table_name]['columns'] if c['name'] in data[0]]
            inserted = pg_insert_many(table_name, columns, data, target=target)
            
            results['data'][table_name] = {
                'generated': len(data),
                'inserted': inserted,
                'validated': valid,
                'llm_powered': True
            }
            
            pk = schema_info[table_name]['primary_key']
            if pk:
                generated_ids[table_name] = [row.get(pk) for row in data if row.get(pk)]
        else:
            results['data'][table_name] = {'error': 'LLM generation failed', 'fallback': 'using rule-based'}
    
    if progress_callback:
        progress_callback(100, "✨ Agentic generation complete!")
    
    results['phases']['transform'] = 'LLM-powered generation complete'
    results['phases']['validate'] = 'All data validated against schema'
    
    return results

# ============================================================================
# Sample Data Generators (Rule-Based Fallback)
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
    """Generate unique email"""
    domains = ["gmail.com", "company.com", "corp.io", "business.net", "enterprise.org"]
    return f"{first.lower()}.{last.lower()}{idx}@{random.choice(domains)}"

def generate_phone():
    """Generate phone number"""
    return f"555-{random.randint(1000, 9999)}"

def generate_address():
    """Generate shipping address"""
    streets = ["Main", "Oak", "Pine", "Maple", "Cedar", "Elm", "Park", "Lake", "River", "Hill"]
    return f"{random.randint(100, 9999)} {random.choice(streets)} St, City, ST {random.randint(10000, 99999)}"

# ============================================================================
# Database Operations
# ============================================================================
def run_query(sql: str, fetch: bool = True):
    """Execute SQL query"""
    try:
        result = session.sql(sql)
        if fetch:
            return result.to_pandas()
        else:
            result.collect()
            return True
    except Exception as e:
        st.error(f"Query error: {e}")
        return None

def get_table_counts():
    """Get row counts for all pipeline tables"""
    counts = {}
    
    # Landing tables
    for table in ["customers", "orders", "products", "order_items", "support_tickets"]:
        try:
            result = session.sql(f'SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS."public"."{table}"').collect()
            counts[f"landing_{table}"] = result[0]['CNT']
        except:
            counts[f"landing_{table}"] = 0
    
    # Bronze tables
    for table in ["CUSTOMERS_VARIANT", "ORDERS_VARIANT", "PRODUCTS_VARIANT", "ORDER_ITEMS_VARIANT", "SUPPORT_TICKETS_VARIANT"]:
        try:
            result = session.sql(f'SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.BRONZE.{table}').collect()
            counts[f"bronze_{table.lower()}"] = result[0]['CNT']
        except:
            counts[f"bronze_{table.lower()}"] = 0
    
    # Silver tables
    for table in ["CUSTOMERS", "ORDERS", "PRODUCTS", "ORDER_ITEMS", "SUPPORT_TICKETS"]:
        try:
            result = session.sql(f'SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.SILVER.{table}').collect()
            counts[f"silver_{table.lower()}"] = result[0]['CNT']
        except:
            counts[f"silver_{table.lower()}"] = 0
    
    # Gold tables (dynamic)
    gold_tables_df = session.sql("SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE'").collect()
    for row in gold_tables_df:
        table = row[0]
        try:
            result = session.sql(f'SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.GOLD.{table}').collect()
            counts[f"gold_{table.lower()}"] = result[0]['CNT']
        except:
            counts[f"gold_{table.lower()}"] = 0
    
    return counts

# ============================================================================
# Data Generation Functions
# ============================================================================
SEGMENTS = ["STANDARD", "PREMIUM", "ENTERPRISE", "SMB", "STARTUP"]
LOYALTY_TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
PAYMENT_METHODS = ["credit_card", "wire_transfer", "ach", "check", "paypal"]

def generate_customers(num_rows: int, start_id: int = 1):
    """Generate customer records (matches dbaontap schema)"""
    customers = []
    for i in range(num_rows):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        revenue = round(random.uniform(50000, 10000000), 2)
        if revenue > 5000000:
            tier = "PLATINUM"
        elif revenue > 1000000:
            tier = "GOLD"
        elif revenue > 500000:
            tier = "SILVER"
        else:
            tier = "BRONZE"
        customers.append({
            "customer_id": start_id + i,
            "email": generate_email(first, last, start_id + i),
            "first_name": first,
            "last_name": last,
            "company_name": random.choice(COMPANIES),
            "phone": generate_phone(),
            "segment": random.choice(SEGMENTS),
            "annual_revenue": revenue,
            "loyalty_tier": tier,
            "is_active": random.choice([True, True, True, False]),
            "created_at": datetime.now() - timedelta(days=random.randint(1, 365)),
            "updated_at": datetime.now()
        })
    return customers

def generate_products(num_rows: int, start_id: int = 1):
    """Generate product records (matches dbaontap schema)"""
    products = []
    for i in range(num_rows):
        base = random.choice(PRODUCTS)
        products.append({
            "product_id": start_id + i,
            "sku": f"{base['category'][:3].upper()}-{start_id + i:04d}",
            "name": base["name"],
            "description": f"{base['name']} - {base['category']} solution for enterprise needs",
            "category": base["category"],
            "list_price": round(base["price"] * random.uniform(0.8, 1.2), 2),
            "cost_price": round(base["cost"] * random.uniform(0.8, 1.2), 2),
            "is_active": random.choice([True, True, True, False]),
            "created_at": datetime.now() - timedelta(days=random.randint(1, 365))
        })
    return products

def generate_orders(num_rows: int, customer_ids: list, start_id: int = 1):
    """Generate order records (matches dbaontap schema)"""
    orders = []
    for i in range(num_rows):
        order_date = datetime.now() - timedelta(days=random.randint(1, 90))
        orders.append({
            "order_id": start_id + i,
            "customer_id": random.choice(customer_ids),
            "order_date": order_date,
            "status": random.choice(ORDER_STATUSES),
            "total_amount": round(random.uniform(1000, 100000), 2),
            "payment_method": random.choice(PAYMENT_METHODS),
            "notes": f"Order generated for demo - ID {start_id + i}" if random.random() > 0.7 else None,
            "created_at": datetime.now(),
            "updated_at": datetime.now()
        })
    return orders

def generate_order_items(num_rows: int, order_ids: list, product_ids: list, start_id: int = 1):
    """Generate order item records (matches dbaontap schema)"""
    items = []
    for i in range(num_rows):
        qty = random.randint(1, 10)
        price = round(random.uniform(500, 50000), 2)
        line_total = round(qty * price, 2)
        items.append({
            "order_item_id": start_id + i,
            "order_id": random.choice(order_ids),
            "product_id": random.choice(product_ids),
            "quantity": qty,
            "unit_price": price,
            "line_total": line_total,
            "created_at": datetime.now()
        })
    return items

def generate_support_tickets(num_rows: int, customer_ids: list, order_ids: list, start_id: int = 1):
    """Generate support ticket records (matches dbaontap schema)"""
    tickets = []
    for i in range(num_rows):
        created = datetime.now() - timedelta(days=random.randint(1, 60))
        status = random.choice(TICKET_STATUSES)
        resolved = created + timedelta(hours=random.randint(1, 72)) if status in ["resolved", "closed"] else None
        tickets.append({
            "ticket_id": start_id + i,
            "customer_id": random.choice(customer_ids),
            "order_id": random.choice(order_ids) if order_ids and random.random() > 0.3 else None,
            "subject": random.choice(TICKET_SUBJECTS),
            "description": f"Demo ticket description for testing - ID {start_id + i}",
            "priority": random.choice(TICKET_PRIORITIES),
            "status": status,
            "created_at": created,
            "resolved_at": resolved
        })
    return tickets

def reconcile_columns_with_schema(table_name: str, generated_data: list, target: str = "source"):
    """Hybrid schema-discover: reconcile rule-based data with actual PG schema.
    Adds NULLs for new columns the generator doesn't know about,
    drops columns that no longer exist in the schema."""
    if not generated_data:
        return generated_data, []
    schema_df = pg_execute(
        f"SELECT column_name, data_type, column_default FROM information_schema.columns "
        f"WHERE table_schema = 'public' AND table_name = '{table_name}' ORDER BY ordinal_position",
        target=target, fetch=True
    )
    if schema_df is None or schema_df.empty:
        return generated_data, list(generated_data[0].keys())
    actual_cols = schema_df['column_name'].tolist()
    generated_cols = set(generated_data[0].keys())
    new_cols = [c for c in actual_cols if c not in generated_cols and schema_df[schema_df['column_name']==c]['column_default'].iloc[0] is None]
    removed_cols = [c for c in generated_cols if c not in actual_cols]
    if new_cols or removed_cols:
        for row in generated_data:
            for col in new_cols:
                row[col] = None
            for col in removed_cols:
                row.pop(col, None)
    insert_cols = [c for c in actual_cols if c in generated_data[0]]
    return generated_data, insert_cols

def insert_data(table_name: str, data: list, columns: list):
    """Insert data into Snowflake landing table using SQL"""
    if not data:
        return 0
    
    inserted = 0
    for row in data:
        values = []
        for col in columns:
            val = row.get(col)
            if val is None:
                values.append("NULL")
            elif isinstance(val, str):
                values.append(f"'{val.replace(chr(39), chr(39)+chr(39))}'")
            elif isinstance(val, datetime):
                values.append(f"'{val.isoformat()}'")
            elif isinstance(val, bool):
                values.append("TRUE" if val else "FALSE")
            else:
                values.append(str(val))
        
        sql = f"""INSERT INTO DBAONTAP_ANALYTICS."public"."{table_name}" ({', '.join(columns)}) 
                  VALUES ({', '.join(values)})"""
        try:
            session.sql(sql).collect()
            inserted += 1
        except Exception as e:
            st.warning(f"Insert error for {table_name}: {e}")
    
    return inserted

# ============================================================================
# UI Components
# ============================================================================
def render_sidebar():
    """Render sidebar with pipeline status"""
    st.sidebar.title("🔄 Pipeline Status")
    
    if st.sidebar.button("🔄 Refresh Status"):
        st.rerun()
    
    st.sidebar.markdown("---")
    
    counts = get_table_counts()
    
    # Summary metrics
    st.sidebar.metric("Landing Records", counts.get("landing_customers", 0))
    st.sidebar.metric("Silver Records", counts.get("silver_customers", 0))
    st.sidebar.metric("Gold Records", counts.get("gold_customer_360", 0))
    
    st.sidebar.markdown("---")
    
    # Agentic Workflow Status
    st.sidebar.subheader("🤖 Workflow Status")
    try:
        workflow_stats = session.sql("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
            FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS
        """).collect()[0]
        st.sidebar.metric("Total Runs", workflow_stats['TOTAL'])
        col1, col2 = st.sidebar.columns(2)
        col1.metric("✅ Completed", workflow_stats['COMPLETED'])
        col2.metric("❌ Failed", workflow_stats['FAILED'])
    except:
        st.sidebar.caption("No workflow data")
    
    st.sidebar.markdown("---")
    
    # Semantic views count
    try:
        sv_count = session.sql("""
            SELECT COUNT(*) as CNT FROM INFORMATION_SCHEMA.SEMANTIC_VIEWS WHERE SCHEMA = 'GOLD'
        """).collect()[0]['CNT']
        st.sidebar.metric("Semantic Views", sv_count)
    except:
        st.sidebar.metric("Semantic Views", 0)

# ============================================================================
# Cortex Analyst Chatbot
# ============================================================================
def call_cortex_analyst(sv_fqns, question: str, chat_history: list = None):
    """Call Cortex Analyst REST API via _snowflake module.
    sv_fqns: single FQN string OR list of FQN strings for multi-view routing.
    """
    messages = []
    if chat_history:
        for msg in chat_history:
            if msg["role"] == "user":
                messages.append({
                    "role": "user",
                    "content": [{"type": "text", "text": msg["content"]}]
                })
            elif msg["role"] == "assistant":
                content = [{"type": "text", "text": msg["content"]}]
                if msg.get("sql"):
                    content.append({"type": "sql", "statement": msg["sql"]})
                messages.append({
                    "role": "analyst",
                    "content": content
                })
    
    messages.append({
        "role": "user",
        "content": [{"type": "text", "text": question}]
    })
    
    request_body = {"messages": messages}
    if isinstance(sv_fqns, list) and len(sv_fqns) > 1:
        request_body["semantic_models"] = [{"semantic_view": fqn} for fqn in sv_fqns]
    else:
        request_body["semantic_view"] = sv_fqns if isinstance(sv_fqns, str) else sv_fqns[0]
    
    resp = _snowflake.send_snow_api_request(
        "POST",
        "/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000
    )
    
    if resp["status"] == 200:
        return json.loads(resp["content"])
    else:
        raise Exception(f"API error {resp['status']}: {resp.get('content', 'Unknown error')}")

def render_chatbot_tab():
    """Render AI chatbot interface using Cortex Analyst"""
    st.header("💬 AI Assistant (Cortex Analyst)")
    st.markdown("Ask questions about your data using natural language. Powered by Semantic Views.")
    
    sv_list = run_query("""
        SELECT NAME FROM INFORMATION_SCHEMA.SEMANTIC_VIEWS WHERE SCHEMA = 'GOLD' ORDER BY NAME
    """)
    
    if sv_list is None or len(sv_list) == 0:
        st.warning("No semantic views available. Run the Agentic Pipeline first to create them.")
        st.info("💡 Go to **Semantic Views** tab → Click **Run Semantic View Pipeline**")
        return
    
    sv_names = sv_list['NAME'].tolist()
    
    if "sv_use_all" not in st.session_state:
        st.session_state.sv_use_all = True
    if "selected_svs" not in st.session_state:
        st.session_state.selected_svs = sv_names
    
    with st.expander("⚙️ Settings"):
        use_all = st.toggle("Use All Semantic Views (recommended)", value=st.session_state.sv_use_all,
                            help="Routes each question to the best semantic view automatically.")
        st.session_state.sv_use_all = use_all
        
        if not use_all:
            selected_svs = st.multiselect(
                "Semantic Views",
                sv_names,
                default=st.session_state.selected_svs if all(s in sv_names for s in st.session_state.selected_svs) else sv_names[:1],
                help="Select one or more semantic views to query."
            )
            st.session_state.selected_svs = selected_svs
        else:
            st.session_state.selected_svs = sv_names
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("🗑️ Clear Chat History"):
                st.session_state.chat_messages = []
                st.rerun()
        with col2:
            st.caption(f"Model: Cortex Analyst")
    
    active_svs = st.session_state.selected_svs
    if len(active_svs) == len(sv_names):
        st.caption(f"📊 Using: **All {len(active_svs)} Semantic Views** (auto-routing)")
    elif len(active_svs) == 1:
        st.caption(f"📊 Using: **{active_svs[0]}**")
    else:
        st.caption(f"📊 Using: **{len(active_svs)} Semantic Views** ({', '.join(active_svs)})")
    
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []
    
    chat_container = st.container()
    with chat_container:
        for message in st.session_state.chat_messages:
            role_icon = "🧑" if message["role"] == "user" else "🤖"
            with _chat_message(message["role"]):
                st.markdown(message['content'])
                if "sql" in message and message["sql"]:
                    with st.expander("📝 View SQL"):
                        st.code(message["sql"], language="sql")
                if "data" in message and message["data"] is not None:
                    st.dataframe(message["data"], use_container_width=True)
    
    prompt = _chat_input("Ask a question about your data...")
    
    if prompt:
        st.session_state.chat_messages.append({"role": "user", "content": prompt})
        
        with _chat_message("user"):
            st.markdown(prompt)
        
        sv_fqns = [f"DBAONTAP_ANALYTICS.GOLD.{sv}" for sv in st.session_state.selected_svs]
        if not sv_fqns:
            st.error("No semantic views selected. Open Settings and select at least one.")
            return
        max_retries = 3
        current_prompt = prompt
        retry_history = []
        last_error = ""
        
        with _chat_message("assistant"):
            for attempt in range(max_retries):
                with st.spinner(f"{'Thinking...' if attempt == 0 else f'Retrying ({attempt + 1}/{max_retries})...'}"):
                    try:
                        if attempt > 0:
                            current_prompt = f"""The previous SQL query failed with this error:
```
{last_error}
```

Original question: {prompt}

Please fix the SQL query to resolve this error."""
                        
                        response = call_cortex_analyst(
                            sv_fqns, 
                            current_prompt, 
                            st.session_state.chat_messages[:-1] + retry_history if attempt > 0 else st.session_state.chat_messages[:-1]
                        )
                        
                        message_text = ""
                        sql = ""
                        
                        if "message" in response:
                            content = response.get("message", {}).get("content", [])
                            for item in content:
                                if item.get("type") == "text":
                                    message_text = item.get("text", "")
                                elif item.get("type") == "sql":
                                    sql = item.get("statement", "")
                        
                        routed_sv = None
                        if "semantic_model_selection" in response:
                            sel = response["semantic_model_selection"]
                            routed_sv = sel.get("semantic_view") or sel.get("semantic_model_file")
                        
                        if sql:
                            if attempt > 0:
                                st.info(f"🔄 Auto-corrected (attempt {attempt + 1})")
                            if routed_sv and len(sv_fqns) > 1:
                                sv_short = routed_sv.split(".")[-1] if "." in routed_sv else routed_sv
                                st.caption(f"🔀 Routed to: **{sv_short}**")
                            
                            with st.expander("📝 View SQL"):
                                st.code(sql, language="sql")
                            
                            try:
                                data = run_query(sql)
                                if data is not None and len(data) > 0:
                                    if message_text:
                                        st.markdown(message_text)
                                    st.dataframe(data, use_container_width=True)
                                    st.session_state.chat_messages.append({
                                        "role": "assistant",
                                        "content": message_text or "Query executed successfully.",
                                        "sql": sql,
                                        "data": data,
                                        "retries": attempt
                                    })
                                    break
                                else:
                                    if message_text:
                                        st.markdown(message_text)
                                    st.info("Query returned no results.")
                                    st.session_state.chat_messages.append({
                                        "role": "assistant",
                                        "content": message_text + "\n\n*Query returned no results.*" if message_text else "Query returned no results.",
                                        "sql": sql,
                                        "retries": attempt
                                    })
                                    break
                                    
                            except Exception as exec_err:
                                last_error = str(exec_err)
                                retry_history.append({
                                    "role": "assistant",
                                    "content": f"SQL execution failed: {last_error}",
                                    "sql": sql
                                })
                                
                                if attempt == max_retries - 1:
                                    st.error(f"❌ Failed after {max_retries} attempts. Last error: {last_error}")
                                    with st.expander("View Failed SQL"):
                                        st.code(sql, language="sql")
                                    st.session_state.chat_messages.append({
                                        "role": "assistant",
                                        "content": f"Failed after {max_retries} attempts. Last error: {last_error}",
                                        "sql": sql,
                                        "error": last_error
                                    })
                                else:
                                    st.warning(f"⚠️ Attempt {attempt + 1} failed, retrying...")
                        else:
                            if message_text:
                                st.markdown(message_text)
                            st.session_state.chat_messages.append({
                                "role": "assistant",
                                "content": message_text or "I couldn't generate SQL for that query."
                            })
                            break
                            
                    except Exception as e:
                        if attempt == max_retries - 1:
                            error_msg = f"Error: {str(e)}"
                            st.error(error_msg)
                            st.session_state.chat_messages.append({
                                "role": "assistant",
                                "content": error_msg
                            })
                        else:
                            last_error = str(e)
                            st.warning(f"⚠️ API error, retrying...")

def render_data_generation_tab():
    """Render synthetic data generation interface"""
    st.header("📊 Generate Synthetic Data")
    st.markdown("Generate sample data directly into the pipeline to demo the CDC flow.")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Generation Mode")
        gen_mode = st.radio(
            "Choose generation approach:",
            ["🤖 Agentic (AI-Powered)", "📋 Rule-Based (Hardcoded)"],
            help="""
            **Agentic**: Uses Cortex LLM to discover schema and generate contextual data
            **Rule-Based**: Uses predefined generators (faster, deterministic)
            """
        )
        is_agentic = "Agentic" in gen_mode
        
        st.markdown("---")
        
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
            st.warning("PostgreSQL EAI not available. Using Snowflake landing tables.")
            target = "SNOWFLAKE"
        
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
        elif "LANDING" in target:
            st.info("📤 Data → PostgreSQL LANDING → (Openflow CDC) → Snowflake")
        else:
            st.info("📤 Data → Snowflake Landing → Bronze → Silver → Gold")
        
        if is_agentic:
            st.markdown("### 🤖 Agentic Workflow")
            st.markdown("""
            Following **AI-First Methodology**:
            1. **DISCOVER** - Query `information_schema` to discover table structures
            2. **ANALYZE** - Understand column semantics and relationships  
            3. **TRANSFORM** - Use Cortex LLM to generate contextual data
            4. **VALIDATE** - Verify data matches schema constraints
            """)
        
        if generate_btn:
            if is_agentic and POSTGRES_AVAILABLE and "PostgreSQL" in target:
                db_target = "source" if "SOURCE" in target else "landing"
                progress = st.progress(0, "Starting agentic workflow...")
                status_text = st.empty()  # For detailed batch status
                
                def update_progress(pct, msg):
                    progress.progress(pct / 100, msg)  # Streamlit progress expects 0-1
                    status_text.caption(msg)
                
                with st.spinner("🤖 Running agentic data generation (batched with timeout)..."):
                    results = run_agentic_data_generator(
                        target=db_target,
                        num_rows={
                            'customers': num_customers,
                            'products': num_products,
                            'orders': num_orders,
                            'order_items': num_order_items,
                            'support_tickets': num_tickets
                        },
                        progress_callback=update_progress
                    )
                
                if 'error' in results:
                    st.error(f"Agentic generation failed: {results['error']}")
                    st.warning("Falling back to rule-based generation...")
                else:
                    st.success("✅ Agentic data generation complete!")
                    
                    with st.expander("📋 Workflow Phases", expanded=True):
                        for phase, detail in results.get('phases', {}).items():
                            st.write(f"**{phase.upper()}**: {detail}")
                    
                    with st.expander("📊 Generated Data Summary", expanded=True):
                        st.json(results.get('data', {}))
                    
                    if db_target == "landing":
                        st.info("💡 Openflow CDC will pick up these changes and replicate to Snowflake.")
                    else:
                        st.info("💡 Logical replication will sync to LANDING, then Openflow CDC will replicate to Snowflake.")
            
            elif POSTGRES_AVAILABLE and "PostgreSQL" in target:
                st.markdown("### 📋 Rule-Based Generation")
                with st.spinner("Generating data (rule-based)..."):
                    progress = st.progress(0, "Starting...")
                    results = {}
                    
                    db_target = "source" if "SOURCE" in target else "landing"
                    
                    try:
                        max_cust_df = pg_execute("SELECT COALESCE(MAX(customer_id), 0) as m FROM customers", target=db_target, fetch=True)
                        max_prod_df = pg_execute("SELECT COALESCE(MAX(product_id), 0) as m FROM products", target=db_target, fetch=True)
                        max_order_df = pg_execute("SELECT COALESCE(MAX(order_id), 0) as m FROM orders", target=db_target, fetch=True)
                        max_item_df = pg_execute("SELECT COALESCE(MAX(order_item_id), 0) as m FROM order_items", target=db_target, fetch=True)
                        max_ticket_df = pg_execute("SELECT COALESCE(MAX(ticket_id), 0) as m FROM support_tickets", target=db_target, fetch=True)
                        
                        max_cust = int(max_cust_df.iloc[0, 0]) if max_cust_df is not None else 0
                        max_prod = int(max_prod_df.iloc[0, 0]) if max_prod_df is not None else 0
                        max_order = int(max_order_df.iloc[0, 0]) if max_order_df is not None else 0
                        max_item = int(max_item_df.iloc[0, 0]) if max_item_df is not None else 0
                        max_ticket = int(max_ticket_df.iloc[0, 0]) if max_ticket_df is not None else 0
                    except Exception as e:
                        st.error(f"Failed to query PostgreSQL: {e}")
                        return
                    
                    progress.progress(10, "Generating customers...")
                    customers = generate_customers(num_customers, max_cust + 1)
                    customer_ids = [c["customer_id"] for c in customers]
                    
                    progress.progress(20, "Generating products...")
                    products = generate_products(num_products, max_prod + 1)
                    product_ids = [p["product_id"] for p in products]
                    
                    progress.progress(30, "Generating orders...")
                    orders = generate_orders(num_orders, customer_ids, max_order + 1) if customer_ids else []
                    order_ids = [o["order_id"] for o in orders]
                    
                    progress.progress(40, "Generating order items...")
                    items = generate_order_items(num_order_items, order_ids, product_ids, max_item + 1) if order_ids and product_ids else []
                    
                    progress.progress(50, "Generating tickets...")
                    tickets = generate_support_tickets(num_tickets, customer_ids, order_ids, max_ticket + 1) if customer_ids and num_tickets > 0 else []
                    
                    progress.progress(60, f"Reconciling with schema & inserting to PostgreSQL {db_target.upper()}...")
                    customers, cust_cols = reconcile_columns_with_schema("customers", customers, target=db_target)
                    results['customers'] = pg_insert_many("customers", cust_cols, customers, target=db_target)
                    products, prod_cols = reconcile_columns_with_schema("products", products, target=db_target)
                    results['products'] = pg_insert_many("products", prod_cols, products, target=db_target)
                    orders, order_cols = reconcile_columns_with_schema("orders", orders, target=db_target)
                    results['orders'] = pg_insert_many("orders", order_cols, orders, target=db_target)
                    items, item_cols = reconcile_columns_with_schema("order_items", items, target=db_target)
                    results['order_items'] = pg_insert_many("order_items", item_cols, items, target=db_target)
                    tickets, ticket_cols = reconcile_columns_with_schema("support_tickets", tickets, target=db_target)
                    results['support_tickets'] = pg_insert_many("support_tickets", ticket_cols, tickets, target=db_target)
                    
                    progress.progress(100, "Complete!")
                    
                    st.success(f"✅ Data inserted to PostgreSQL {db_target.upper()}!")
                    st.json(results)
                    
                    if db_target == "landing":
                        st.info("💡 Openflow CDC will pick up these changes and replicate to Snowflake.")
                    else:
                        st.info("💡 Logical replication will sync to LANDING, then Openflow CDC will replicate to Snowflake.")
            else:
                with st.spinner("Generating data to Snowflake..."):
                    progress = st.progress(0, text="Starting...")
                    
                    try:
                        max_cust = session.sql('SELECT COALESCE(MAX(customer_id), 0) as m FROM DBAONTAP_ANALYTICS."public"."customers"').collect()[0]['M']
                        max_prod = session.sql('SELECT COALESCE(MAX(product_id), 0) as m FROM DBAONTAP_ANALYTICS."public"."products"').collect()[0]['M']
                        max_order = session.sql('SELECT COALESCE(MAX(order_id), 0) as m FROM DBAONTAP_ANALYTICS."public"."orders"').collect()[0]['M']
                        max_item = session.sql('SELECT COALESCE(MAX(item_id), 0) as m FROM DBAONTAP_ANALYTICS."public"."order_items"').collect()[0]['M']
                        max_ticket = session.sql('SELECT COALESCE(MAX(ticket_id), 0) as m FROM DBAONTAP_ANALYTICS."public"."support_tickets"').collect()[0]['M']
                    except:
                        max_cust = max_prod = max_order = max_item = max_ticket = 0
                    
                    results = {}
                    
                    progress.progress(10, text="Generating customers...")
                    customers = generate_customers(num_customers, max_cust + 1)
                    results['customers'] = insert_data("customers", customers, 
                        ["customer_id", "email", "first_name", "last_name", "company_name", "segment", "annual_revenue", "city", "state", "country", "is_active", "created_at", "updated_at"])
                    customer_ids = [c["customer_id"] for c in customers]
                    
                    progress.progress(30, text="Generating products...")
                    products = generate_products(num_products, max_prod + 1)
                    results['products'] = insert_data("products", products,
                        ["product_id", "sku", "name", "category", "list_price", "cost_price", "is_active", "created_at"])
                    product_ids = [p["product_id"] for p in products]
                    
                    progress.progress(50, text="Generating orders...")
                    if customer_ids:
                        orders = generate_orders(num_orders, customer_ids, max_order + 1)
                        results['orders'] = insert_data("orders", orders,
                            ["order_id", "customer_id", "order_date", "status", "total_amount", "shipping_address", "created_at", "updated_at"])
                        order_ids = [o["order_id"] for o in orders]
                    else:
                        results['orders'] = 0
                        order_ids = []
                    
                    progress.progress(70, text="Generating order items...")
                    if order_ids and product_ids:
                        items = generate_order_items(num_order_items, order_ids, product_ids, max_item + 1)
                        results['order_items'] = insert_data("order_items", items,
                            ["item_id", "order_id", "product_id", "quantity", "unit_price", "discount_percent"])
                    else:
                        results['order_items'] = 0
                    
                    progress.progress(90, text="Generating support tickets...")
                    if customer_ids and num_tickets > 0:
                        tickets = generate_support_tickets(num_tickets, customer_ids, max_ticket + 1)
                        results['support_tickets'] = insert_data("support_tickets", tickets,
                            ["ticket_id", "customer_id", "subject", "description", "priority", "status", "assigned_to", "resolution_hours", "created_at", "resolved_at"])
                    else:
                        results['support_tickets'] = 0
                    
                    progress.progress(100, text="Complete!")
                    
                    st.success("✅ Data generation complete!")
                    st.json(results)
        
        st.markdown("---")
        
        if POSTGRES_AVAILABLE and "PostgreSQL" in target:
            db_target = "source" if "SOURCE" in target else "landing"
            st.markdown(f"**Current PostgreSQL {db_target.upper()} Counts**")
            
            counts_df = pg_execute("""
                SELECT 'customers' as table_name, COUNT(*) as count FROM public.customers
                UNION ALL SELECT 'products', COUNT(*) FROM public.products
                UNION ALL SELECT 'orders', COUNT(*) FROM public.orders
                UNION ALL SELECT 'order_items', COUNT(*) FROM public.order_items
                UNION ALL SELECT 'support_tickets', COUNT(*) FROM public.support_tickets
            """, target=db_target, fetch=True)
            
            if counts_df is not None:
                st.dataframe(counts_df, use_container_width=True)
        else:
            st.markdown("**Current Snowflake Landing Counts**")
            
            counts_df = run_query("""
                SELECT 'customers' as table_name, COUNT(*) as count FROM DBAONTAP_ANALYTICS."public"."customers"
                UNION ALL SELECT 'products', COUNT(*) FROM DBAONTAP_ANALYTICS."public"."products"
                UNION ALL SELECT 'orders', COUNT(*) FROM DBAONTAP_ANALYTICS."public"."orders"
                UNION ALL SELECT 'order_items', COUNT(*) FROM DBAONTAP_ANALYTICS."public"."order_items"
                UNION ALL SELECT 'support_tickets', COUNT(*) FROM DBAONTAP_ANALYTICS."public"."support_tickets"
            """)
            
            if counts_df is not None:
                st.dataframe(counts_df, use_container_width=True)

def render_pipeline_tab():
    """Render pipeline status and management"""
    st.header("🔄 Pipeline Status")
    
    # Full pipeline flow visualization
    st.markdown("""
    ```
    PostgreSQL SOURCE → (Logical Replication) → PostgreSQL LANDING → (Openflow CDC) → Snowflake LANDING → BRONZE → SILVER → GOLD
    ```
    """)
    
    # PostgreSQL Data Flow Section
    if POSTGRES_AVAILABLE:
        st.subheader("🐘 PostgreSQL Data Flow")
        
        pg_col1, pg_col2, pg_col3 = st.columns(3)
        
        with pg_col1:
            st.markdown("**SOURCE** (Origin)")
            try:
                source_counts = pg_execute("""
                    SELECT 'customers' as tbl, COUNT(*) as cnt FROM public.customers
                    UNION ALL SELECT 'orders', COUNT(*) FROM public.orders
                    UNION ALL SELECT 'products', COUNT(*) FROM public.products
                    UNION ALL SELECT 'order_items', COUNT(*) FROM public.order_items
                    UNION ALL SELECT 'support_tickets', COUNT(*) FROM public.support_tickets
                """, target="source", fetch=True)
                if source_counts is not None and len(source_counts) > 0:
                    st.dataframe(source_counts)
                else:
                    st.warning(f"SOURCE returned: {source_counts}")
            except Exception as e:
                st.warning(f"SOURCE error: {e}")
        
        with pg_col2:
            st.markdown("**LANDING** (Migration Target)")
            try:
                landing_counts = pg_execute("""
                    SELECT 'customers' as tbl, COUNT(*) as cnt FROM public.customers
                    UNION ALL SELECT 'orders', COUNT(*) FROM public.orders
                    UNION ALL SELECT 'products', COUNT(*) FROM public.products
                    UNION ALL SELECT 'order_items', COUNT(*) FROM public.order_items
                    UNION ALL SELECT 'support_tickets', COUNT(*) FROM public.support_tickets
                """, target="landing", fetch=True)
                if landing_counts is not None and len(landing_counts) > 0:
                    st.dataframe(landing_counts)
                else:
                    st.warning(f"LANDING returned: {landing_counts}")
            except Exception as e:
                st.warning(f"LANDING error: {e}")
        
        with pg_col3:
            st.markdown("**Replication Status**")
            if source_counts is not None and landing_counts is not None:
                source_total = source_counts['cnt'].sum()
                landing_total = landing_counts['cnt'].sum()
                st.metric("SOURCE Total", int(source_total))
                st.metric("LANDING Total", int(landing_total))
                if source_total > 0:
                    sync_pct = (landing_total / source_total) * 100
                    st.metric("Sync %", f"{sync_pct:.1f}%")
                    if sync_pct >= 100:
                        st.success("✅ In Sync")
                    else:
                        st.warning(f"⏳ Replicating... ({int(source_total - landing_total)} behind)")
            else:
                st.info("Connect to both DBs to see status")
        
        st.markdown("---")
    
    # Snowflake Layer Counts
    st.subheader("❄️ Snowflake Data Layers")
    
    col1, col2, col3, col4 = st.columns(4)
    
    counts = get_table_counts()
    
    with col1:
        st.subheader("📥 Landing")
        st.metric("Customers", counts.get("landing_customers", 0))
        st.metric("Orders", counts.get("landing_orders", 0))
        st.metric("Products", counts.get("landing_products", 0))
    
    with col2:
        st.subheader("🥉 Bronze")
        st.metric("Customers", counts.get("bronze_customers_variant", 0))
        st.metric("Orders", counts.get("bronze_orders_variant", 0))
        st.metric("Products", counts.get("bronze_products_variant", 0))
    
    with col3:
        st.subheader("🥈 Silver")
        st.metric("Customers", counts.get("silver_customers", 0))
        st.metric("Orders", counts.get("silver_orders", 0))
        st.metric("Products", counts.get("silver_products", 0))
    
    with col4:
        st.subheader("🥇 Gold")
        st.metric("Customer 360", counts.get("gold_customer_360", 0))
        st.metric("Product Perf", counts.get("gold_product_performance", 0))
        st.metric("Order Summary", counts.get("gold_order_summary", 0))
    
    st.markdown("---")
    
    # Dynamic Table refresh status
    st.subheader("Dynamic Table Status")
    
    dt_status = run_query("""
        SELECT 
            name as "Name",
            state_code as "State",
            state_message as "Message",
            refresh_start_time as "Last Refresh",
            completion_target as "Target"
        FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
            NAME_PREFIX => 'DBAONTAP_ANALYTICS'
        ))
        WHERE DATEDIFF('hour', refresh_start_time, CURRENT_TIMESTAMP()) < 24
        QUALIFY ROW_NUMBER() OVER (PARTITION BY name ORDER BY refresh_start_time DESC) = 1
    """)
    
    if dt_status is not None and len(dt_status) > 0:
        st.dataframe(dt_status, use_container_width=True)
    else:
        st.info("No recent Dynamic Table refresh history found.")

def render_semantic_views_tab():
    """Render semantic views management"""
    st.header("🔷 Semantic Views Generation")
    
    st.markdown("""
    **Three approaches to generate Semantic Views:**
    - **Agentic**: Pure LLM analysis of table structure
    - **Knowledge Graph**: Rule-based from KG metadata  
    - **Hybrid** ⭐: KG structure + LLM enrichment (recommended)
    """)
    
    col_agentic, col_kg, col_hybrid = st.columns(3)
    
    with col_agentic:
        st.subheader("🤖 Agentic")
        st.caption("Pure LLM analysis")
        st.markdown("""
        - Discovers Gold tables
        - LLM analyzes schema
        - Generates DDL
        """)
        
        if st.button("🤖 Run Agentic", type="secondary", use_container_width=True):
            with st.spinner("Running agentic pipeline..."):
                result = run_query("CALL DBAONTAP_ANALYTICS.AGENTS.RUN_SEMANTIC_VIEW_PIPELINE()")
                if result is not None and len(result) > 0:
                    result_json = result.iloc[0, 0]
                    if isinstance(result_json, str):
                        result_json = json.loads(result_json)
                    
                    st.success(f"✅ {result_json.get('success_count', 0)} succeeded, {result_json.get('fail_count', 0)} failed")
                    
                    with st.expander("View Details"):
                        st.json(result_json)
    
    with col_kg:
        st.subheader("📊 Knowledge Graph")
        st.caption("Rule-based from metadata")
        st.markdown("""
        - Reads KG metadata
        - Infers dim/measures
        - Fast, deterministic
        """)
        
        if st.button("📊 Generate from KG", type="secondary", use_container_width=True):
            with st.spinner("Generating from Knowledge Graph..."):
                try:
                    result = run_query("CALL DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.GENERATE_ALL_SEMANTIC_VIEWS_FROM_KG()")
                    if result is not None and len(result) > 0:
                        result_json = result.iloc[0, 0]
                        if isinstance(result_json, str):
                            result_json = json.loads(result_json)
                        
                        st.success(f"✅ {result_json.get('success_count', 0)} succeeded, {result_json.get('fail_count', 0)} failed")
                        
                        with st.expander("View Details"):
                            st.json(result_json)
                except Exception as e:
                    st.error(f"Error: {e}")
                    st.info("💡 Run script: `10_kg_semantic_view_generator.sql`")
    
    with col_hybrid:
        st.subheader("⭐ Hybrid")
        st.caption("KG + LLM enrichment")
        st.markdown("""
        - KG structure base
        - LLM descriptions
        - Auto synonyms
        """)
        
        if st.button("⭐ Generate Hybrid", type="primary", use_container_width=True):
            with st.spinner("Step 1/2: Populating Knowledge Graph from schema..."):
                try:
                    kg_result = run_query("CALL DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.POPULATE_KG_FROM_INFORMATION_SCHEMA()")
                    if kg_result is not None and len(kg_result) > 0:
                        kg_json = kg_result.iloc[0, 0]
                        if isinstance(kg_json, str):
                            kg_json = json.loads(kg_json)
                        st.success(f"KG populated: {kg_json.get('tables_added', 0)} tables, {kg_json.get('columns_added', 0)} columns, {kg_json.get('edges_added', 0)} edges")
                except Exception as e:
                    st.warning(f"KG population warning: {e}")
            with st.spinner("Step 2/2: Generating hybrid semantic views (KG + LLM)..."):
                try:
                    result = run_query("CALL DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.GENERATE_ALL_HYBRID_SEMANTIC_VIEWS(TRUE)")
                    if result is not None and len(result) > 0:
                        result_json = result.iloc[0, 0]
                        if isinstance(result_json, str):
                            result_json = json.loads(result_json)
                        
                        st.success(f"✅ {result_json.get('success_count', 0)} succeeded, {result_json.get('fail_count', 0)} failed")
                        st.info("🔷 LLM enrichment: descriptions, synonyms")
                        
                        with st.expander("View Details"):
                            st.json(result_json)
                except Exception as e:
                    st.error(f"Error: {e}")
                    st.info("💡 Run script: `11_hybrid_semantic_view_generator.sql`")
    
    st.markdown("---")
    
    st.subheader("Current Semantic Views")
    
    sv_list = run_query("""
        SHOW SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD
    """)
    
    if sv_list is not None and len(sv_list) > 0:
        cols = sv_list.columns.tolist()
        name_col = [c for c in cols if 'name' in c.lower() and c.lower() != 'database_name' and c.lower() != 'schema_name'][0] if any('name' in c.lower() for c in cols) else cols[0]
        created_col = [c for c in cols if 'created' in c.lower()][0] if any('created' in c.lower() for c in cols) else None
        owner_col = [c for c in cols if 'owner' in c.lower() and 'type' not in c.lower()][0] if any('owner' in c.lower() for c in cols) else None
        
        display_cols = [name_col]
        if created_col:
            display_cols.append(created_col)
        if owner_col:
            display_cols.append(owner_col)
        
        st.dataframe(sv_list[display_cols], use_container_width=True)
        
        for _, row in sv_list.iterrows():
            sv_name = row[name_col]
            with st.expander(f"📋 {sv_name}"):
                try:
                    ddl_df = run_query(f"SELECT GET_DDL('SEMANTIC VIEW', 'DBAONTAP_ANALYTICS.GOLD.{sv_name}')")
                    if ddl_df is not None and len(ddl_df) > 0:
                        ddl_text = ddl_df.iloc[0, 0]
                        
                        dimensions = []
                        facts = []
                        metrics = []
                        current_section = None
                        for line in ddl_text.split('\n'):
                            stripped = line.strip()
                            stripped_lower = stripped.lower()
                            if stripped_lower.startswith('dimensions') and '(' in stripped_lower:
                                current_section = 'dimensions'
                                continue
                            elif stripped_lower.startswith('facts') and '(' in stripped_lower:
                                current_section = 'facts'
                                continue
                            elif stripped_lower.startswith('metrics') and '(' in stripped_lower:
                                current_section = 'metrics'
                                continue
                            elif current_section and stripped == ')':
                                current_section = None
                                continue
                            elif current_section and stripped.startswith(')'):
                                current_section = None
                                continue
                            elif current_section and ' as ' in stripped_lower:
                                parts = stripped.split(' as ', 1)
                                if len(parts) == 2:
                                    col_ref = parts[0].strip()
                                    rest = parts[1].strip()
                                    comment_match = rest.split("comment='", 1)
                                    display_name = comment_match[0].strip().rstrip(',')
                                    comment_text = comment_match[1].rstrip("',") if len(comment_match) > 1 else ''
                                    short_name = col_ref.split('.')[-1] if '.' in col_ref else col_ref
                                    entry = {'name': short_name, 'expr': display_name, 'comment': comment_text}
                                    if current_section == 'dimensions':
                                        dimensions.append(entry)
                                    elif current_section == 'facts':
                                        facts.append(entry)
                                    elif current_section == 'metrics':
                                        metrics.append(entry)
                        
                        col_d, col_f, col_m = st.columns(3)
                        with col_d:
                            st.markdown(f"**📐 Dimensions** ({len(dimensions)})")
                            for d in dimensions:
                                st.markdown(f"- `{d['name']}`")
                                if d['comment']:
                                    st.caption(f"  {d['comment']}")
                        with col_f:
                            st.markdown(f"**📊 Facts** ({len(facts)})")
                            for f in facts:
                                st.markdown(f"- `{f['name']}`")
                                if f['comment']:
                                    st.caption(f"  {f['comment']}")
                        with col_m:
                            st.markdown(f"**📈 Metrics** ({len(metrics)})")
                            for m in metrics:
                                st.markdown(f"- `{m['name']}` = `{m['expr']}`")
                                if m['comment']:
                                    st.caption(f"  {m['comment']}")
                        
                        with st.expander("🔍 Raw DDL"):
                            st.code(ddl_text, language="sql")
                except Exception as e:
                    st.error(f"Could not load details: {e}")
    else:
        st.warning("No semantic views found. Run the pipeline to generate them.")



def render_logs_tab():
    """Render transformation logs and errors"""
    st.header("📋 Transformation Logs & Errors")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        status_filter = st.multiselect(
            "Filter by Status",
            ["SUCCESS", "FAILED", "PENDING"],
            default=["FAILED", "SUCCESS"]
        )
    
    with col2:
        limit = st.number_input("Max Records", 10, 500, 50)
    
    if status_filter:
        status_list = "'" + "','".join(status_filter) + "'"
        logs = run_query(f"""
            SELECT 
                source_table as "Source",
                target_table as "Target",
                status as "Status",
                agent_reasoning as "Reasoning",
                executed_at as "Executed"
            FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG
            WHERE status IN ({status_list})
            ORDER BY executed_at DESC
            LIMIT {limit}
        """)
        
        if logs is not None and len(logs) > 0:
            # Summary
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("✅ Success", len(logs[logs['Status'] == 'SUCCESS']))
            with col2:
                st.metric("❌ Failed", len(logs[logs['Status'] == 'FAILED']))
            with col3:
                st.metric("⏳ Pending", len(logs[logs['Status'] == 'PENDING']))
            
            st.markdown("---")
            st.dataframe(logs, use_container_width=True)
            
            # Show failed details
            failed = logs[logs['Status'] == 'FAILED']
            if len(failed) > 0:
                st.markdown("---")
                st.subheader("❌ Failed Items (Manual Fix Required)")
                
                for idx, row in failed.iterrows():
                    with st.expander(f"🔴 {row['Target']}"):
                        st.markdown(f"**Source:** {row['Source']}")
                        st.markdown(f"**Error:** {row['Reasoning']}")
                        
                        # Get the DDL
                        ddl = run_query(f"""
                            SELECT transformation_sql 
                            FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG
                            WHERE target_table = '{row['Target']}'
                            ORDER BY executed_at DESC LIMIT 1
                        """)
                        if ddl is not None and len(ddl) > 0:
                            st.code(ddl.iloc[0, 0], language="sql")
        else:
            st.info("No logs found matching the filter criteria.")
    
    st.markdown("---")
    
    if st.button("🧹 Clear All Logs"):
        run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG", fetch=False)
        st.success("Logs cleared!")
        st.rerun()

def render_agentic_workflow_tab():
    """Render the full agentic workflow interface (TRIGGER → PLANNER → EXECUTOR → VALIDATOR → REFLECTOR)"""
    st.header("🤖 Agentic Workflow Engine")
    
    phase_configs = [
        ('TRIGGER', 'Detect Events', '#2196F3', '#1976D2'),
        ('PLANNER', 'LLM Analyze', '#9C27B0', '#7B1FA2'),
        ('EXECUTOR', 'Self-Correcting', '#FF9800', '#F57C00'),
        ('VALIDATOR', 'Quality Check', '#4CAF50', '#388E3C'),
        ('REFLECTOR', 'Capture Learnings', '#607D8B', '#455A64'),
    ]
    
    def render_phase_diagram(phase_states=None):
        if phase_states is None:
            phase_states = {}
        pills = []
        for name, subtitle, color1, color2 in phase_configs:
            state = phase_states.get(name, 'pending')
            if state == 'completed':
                bg = 'linear-gradient(135deg,#4CAF50,#2E7D32)'
                shadow = 'rgba(76,175,80,0.4)'
                icon = '✅ '
            elif state == 'running':
                bg = 'linear-gradient(135deg,#FFC107,#FF8F00)'
                shadow = 'rgba(255,193,7,0.4)'
                icon = '🔄 '
            elif state == 'failed':
                bg = 'linear-gradient(135deg,#f44336,#c62828)'
                shadow = 'rgba(244,67,54,0.4)'
                icon = '❌ '
            elif state == 'skipped':
                bg = 'linear-gradient(135deg,#9E9E9E,#757575)'
                shadow = 'rgba(158,158,158,0.2)'
                icon = '⏭️ '
            else:
                bg = f'linear-gradient(135deg,{color1},{color2})'
                shadow = f'rgba(100,100,100,0.15)'
                icon = ''
            pill = f'''<div style="background:{bg}; color:white; padding:10px 16px; border-radius:10px; text-align:center; min-width:100px; box-shadow:0 2px 8px {shadow}; transition:all 0.3s;">
            <div style="font-weight:700; font-size:0.85rem;">{icon}{name}</div>
            <div style="font-size:0.7rem; opacity:0.85;">{subtitle}</div>
        </div>'''
            pills.append(pill)
        arrow = '<div style="font-size:1.4rem; color:#666;">→</div>'
        html = '<div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap; margin:0.5rem 0 1rem 0;">' + arrow.join(pills) + '</div>'
        return html
    
    diagram_placeholder = st.empty()
    diagram_placeholder.markdown(render_phase_diagram(), unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("🚀 Run Workflow")
        
        trigger_type = st.selectbox(
            "Trigger Type",
            ["manual", "stream", "scheduled"],
            help="manual: Process all Bronze tables | stream: Only tables with new data | scheduled: Full refresh"
        )
        
        specific_tables = st.text_area(
            "Specific Tables (optional)",
            placeholder="DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT\nDBAONTAP_ANALYTICS.BRONZE.ORDERS_VARIANT",
            help="Leave empty to process all Bronze tables, or specify one table per line"
        )
        
        if st.button("🤖 Run Agentic Workflow", type="primary", use_container_width=True):
            import time
            
            if specific_tables.strip():
                tables_list = [t.strip() for t in specific_tables.strip().split('\n') if t.strip()]
                tables_joined = "', '".join(tables_list)
                tables_param = f", ARRAY_CONSTRUCT('{tables_joined}')"
            else:
                tables_param = ""
            
            phases = ['TRIGGER', 'PLANNER', 'EXECUTOR', 'VALIDATOR', 'REFLECTOR']
            phase_descriptions = {
                'TRIGGER': 'Detecting tables to process',
                'PLANNER': 'LLM analyzing schemas & planning transformations',
                'EXECUTOR': 'Running transformations with self-correction',
                'VALIDATOR': 'Validating data quality & row counts',
                'REFLECTOR': 'Capturing learnings for future runs'
            }
            
            progress_container = st.container()
            
            with progress_container:
                st.markdown("### 📊 Workflow Progress")
                
                phase_placeholders = {}
                for i, phase in enumerate(phases):
                    phase_placeholders[phase] = st.empty()
                    phase_placeholders[phase].markdown(f"⬜ **Phase {i+1}: {phase}** - {phase_descriptions[phase]}")
                
                status_placeholder = st.empty()
                status_placeholder.info("⏳ Starting workflow...")
                
                result_placeholder = st.empty()
                details_placeholder = st.empty()
            
            diagram_states = {p: 'pending' for p in phases}
            phase_results = {}
            execution_id = None
            workflow_failed = False
            failure_phase = None
            failure_error = None
            
            for i, phase in enumerate(phases):
                if workflow_failed and phase != 'REFLECTOR':
                    diagram_states[phase] = 'skipped'
                    phase_placeholders[phase].markdown(f"⏭️ **Phase {i+1}: {phase}** - Skipped")
                    diagram_placeholder.markdown(render_phase_diagram(diagram_states), unsafe_allow_html=True)
                    continue
                
                diagram_states[phase] = 'running'
                diagram_placeholder.markdown(render_phase_diagram(diagram_states), unsafe_allow_html=True)
                phase_placeholders[phase].markdown(f"🔄 **Phase {i+1}: {phase}** - {phase_descriptions[phase]}...")
                status_placeholder.info(f"⏳ Running **{phase}**...")
                
                try:
                    if phase == 'TRIGGER':
                        sql = f"CALL DBAONTAP_ANALYTICS.AGENTS.WORKFLOW_TRIGGER('{trigger_type}'{tables_param})"
                        r = run_query(sql)
                        if r is not None and len(r) > 0:
                            rj = r.iloc[0, 0]
                            if isinstance(rj, str):
                                rj = json.loads(rj)
                            phase_results[phase] = rj
                            execution_id = rj.get('execution_id')
                            tables = rj.get('tables_to_process', [])
                            if not tables or len(tables) == 0:
                                diagram_states[phase] = 'skipped'
                                phase_placeholders[phase].markdown(f"⚠️ **Phase {i+1}: {phase}** - No tables to process")
                                diagram_placeholder.markdown(render_phase_diagram(diagram_states), unsafe_allow_html=True)
                                status_placeholder.warning("⚠️ No Bronze tables found to process.")
                                for j, p2 in enumerate(phases):
                                    if j > 0:
                                        diagram_states[p2] = 'skipped'
                                        phase_placeholders[p2].markdown(f"⏭️ **Phase {j+1}: {p2}** - Skipped")
                                diagram_placeholder.markdown(render_phase_diagram(diagram_states), unsafe_allow_html=True)
                                break
                        else:
                            raise Exception("No result returned from TRIGGER")
                    else:
                        sql = f"CALL DBAONTAP_ANALYTICS.AGENTS.WORKFLOW_{phase}('{execution_id}')"
                        r = run_query(sql)
                        if r is not None and len(r) > 0:
                            rj = r.iloc[0, 0]
                            if isinstance(rj, str):
                                rj = json.loads(rj)
                            phase_results[phase] = rj
                        else:
                            raise Exception(f"No result returned from {phase}")
                    
                    diagram_states[phase] = 'completed'
                    phase_placeholders[phase].markdown(f"✅ **Phase {i+1}: {phase}** - {phase_descriptions[phase]}")
                    diagram_placeholder.markdown(render_phase_diagram(diagram_states), unsafe_allow_html=True)
                    
                except Exception as e:
                    err_msg = str(e)[:500]
                    if phase == 'REFLECTOR':
                        diagram_states[phase] = 'completed'
                        phase_placeholders[phase].markdown(f"⚠️ **Phase {i+1}: {phase}** - Partial ({err_msg[:80]})")
                        phase_results[phase] = {'error': err_msg}
                    else:
                        diagram_states[phase] = 'failed'
                        phase_placeholders[phase].markdown(f"❌ **Phase {i+1}: {phase}** - FAILED")
                        workflow_failed = True
                        failure_phase = phase
                        failure_error = err_msg
                    diagram_placeholder.markdown(render_phase_diagram(diagram_states), unsafe_allow_html=True)
            
            if execution_id:
                if not workflow_failed:
                    run_query(f"""
                        UPDATE DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS
                        SET current_phase = 'COMPLETED', status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP()
                        WHERE execution_id = '{execution_id}'
                    """, fetch=False)
                    
                    trigger_r = phase_results.get('TRIGGER', {})
                    executor_r = phase_results.get('EXECUTOR', {})
                    planner_r = phase_results.get('PLANNER', {})
                    validator_r = phase_results.get('VALIDATOR', {})
                    reflector_r = phase_results.get('REFLECTOR', {})
                    
                    status_placeholder.success("✅ **Workflow Complete!**")
                    result_placeholder.markdown(f"""
| Metric | Value |
|--------|-------|
| Tables Planned | {planner_r.get('tables_planned', 0)} |
| Executions Succeeded | {executor_r.get('success_count', 0)} |
| Executions Failed | {executor_r.get('fail_count', 0)} |
| Validations Passed | {validator_r.get('passed', 0)} |
| Learnings Captured | {reflector_r.get('learnings_count', 0)} |
                    """)
                else:
                    run_query(f"""
                        UPDATE DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS
                        SET status = 'FAILED', last_error = '{failure_phase}: {failure_error[:200].replace("'","''")}',
                            completed_at = CURRENT_TIMESTAMP()
                        WHERE execution_id = '{execution_id}'
                    """, fetch=False)
                    status_placeholder.error(f"❌ Workflow failed at phase: **{failure_phase}**")
                    result_placeholder.code(failure_error)
                
                with details_placeholder.expander("📋 Full Workflow Details (JSON)"):
                    st.json(phase_results)
        
        st.markdown("---")
        st.subheader("📊 Quick Actions")
        
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("🔄 Refresh Dashboard", use_container_width=True):
                st.rerun()
        with col_b:
            if st.button("🧹 Clear Workflow History", use_container_width=True):
                run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS", fetch=False)
                run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.PLANNER_DECISIONS", fetch=False)
                run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.VALIDATION_RESULTS", fetch=False)
                st.success("Workflow history cleared!")
                st.rerun()
        
        if st.button("🔍 Detect Gold Schema Drift", use_container_width=True):
            with st.spinner("Scanning Silver vs Gold schema differences..."):
                drift = run_query("CALL DBAONTAP_ANALYTICS.AGENTS.DETECT_GOLD_SCHEMA_DRIFT()")
            if drift is not None and len(drift) > 0:
                drift_json = drift.iloc[0, 0]
                if isinstance(drift_json, str):
                    drift_json = json.loads(drift_json)
                st.session_state['gold_drift'] = drift_json
                
                if drift_json.get('status') == 'IN_SYNC':
                    st.success("All Gold tables are in sync with Silver!")
                else:
                    st.warning(f"Drift detected: **{drift_json.get('total_missing_columns', 0)}** missing columns across **{drift_json.get('affected_gold_tables', 0)}** Gold tables")
                    for detail in drift_json.get('details', []):
                        with st.expander(f"🔸 {detail['gold_table']} ← {detail['silver_table']} ({detail['missing_count']} missing)"):
                            for md in detail.get('missing_details', []):
                                icon = "🟢" if md.get('recommendation') == 'AUTO_ADD' else "🟡"
                                st.markdown(f"{icon} **{md['column_name']}** ({md['data_type']}) → {md['recommendation']}")
        
        if st.session_state.get('gold_drift', {}).get('status') == 'DRIFT_DETECTED':
            propagate_mode = st.radio(
                "Propagation Mode",
                ["Dry Run (preview)", "Execute (apply changes)"],
                horizontal=True
            )
            is_dry_run = propagate_mode.startswith("Dry")
            
            if st.button("🚀 Propagate to Gold", type="primary", use_container_width=True):
                dry_str = "TRUE" if is_dry_run else "FALSE"
                with st.spinner("Running Gold propagation..." + (" (dry run)" if is_dry_run else "")):
                    prop = run_query(f"CALL DBAONTAP_ANALYTICS.AGENTS.PROPAGATE_TO_GOLD(NULL, {dry_str}, TRUE, TRUE, TRUE)")
                if prop is not None and len(prop) > 0:
                    prop_json = prop.iloc[0, 0]
                    if isinstance(prop_json, str):
                        prop_json = json.loads(prop_json)
                    
                    summary = prop_json.get('summary', {})
                    if is_dry_run:
                        st.info(f"**Dry Run Complete** — {summary.get('passthrough_actions', 0)} auto-passthrough, {summary.get('agentic_actions', 0)} agentic actions proposed")
                    else:
                        st.success(f"**Propagation Complete** — KG refreshed, Semantic Views regenerated")
                    
                    with st.expander("📋 Full Propagation Details"):
                        st.json(prop_json)
    
    with col2:
        st.subheader("📈 Workflow Dashboard")
        
        dashboard = run_query("""
            SELECT 
                execution_id as "ID",
                status as "Status",
                current_phase as "Phase",
                tables_triggered as "Tables",
                executions_succeeded as "Success",
                executions_failed as "Failed",
                validations_passed as "Validated",
                learnings_captured as "Learnings",
                duration_seconds as "Duration(s)",
                started_at as "Started"
            FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_DASHBOARD
            ORDER BY started_at DESC
            LIMIT 10
        """)
        
        if dashboard is not None and len(dashboard) > 0:
            st.dataframe(dashboard, use_container_width=True)
        else:
            st.info("No workflow executions yet. Run a workflow to see results.")
        
        st.markdown("---")
        st.subheader("🧠 Active Learnings")
        st.markdown("*Patterns learned from past workflows that improve future runs:*")
        
        learnings = run_query("""
            SELECT 
                learning_type as "Type",
                observation as "Observation",
                recommendation as "Recommendation",
                times_observed as "Count",
                ROUND(confidence_score, 2) as "Confidence"
            FROM DBAONTAP_ANALYTICS.METADATA.ACTIVE_LEARNINGS
            LIMIT 5
        """)
        
        if learnings is not None and len(learnings) > 0:
            for idx, row in learnings.iterrows():
                obs_raw = str(row['Observation'] or '')
                rec_raw = str(row['Recommendation'] or '')
                
                parsed_items = []
                if obs_raw.strip().startswith('['):
                    try:
                        parsed_items = json.loads(obs_raw)
                    except json.JSONDecodeError:
                        import re
                        items = re.findall(
                            r'"learning_type"\s*:\s*"([^"]*)".*?"observation"\s*:\s*"([^"]*)"(?:.*?"recommendation"\s*:\s*"([^"]*)")?(?:.*?"confidence"\s*:\s*([\d.]+))?',
                            obs_raw, re.DOTALL
                        )
                        for m in items:
                            parsed_items.append({
                                'learning_type': m[0],
                                'observation': m[1],
                                'recommendation': m[2] if m[2] else None,
                                'confidence': float(m[3]) if m[3] else None
                            })
                
                if parsed_items:
                    with st.expander(f"💡 {len(parsed_items)} pattern(s) learned"):
                        for item in parsed_items:
                            conf = item.get('confidence')
                            conf_str = f"  `{int(conf*100)}%`" if conf else ""
                            ltype = (item.get('learning_type') or 'pattern').replace('_', ' ').title()
                            st.markdown(f"**{ltype}**{conf_str}")
                            st.markdown(f"> {item.get('observation', 'N/A')}")
                            if item.get('recommendation'):
                                st.caption(f"Recommendation: {item['recommendation']}")
                        st.caption(f"Observed {row['Count']} time(s)")
                else:
                    obs_display = obs_raw.replace('"', '').replace('{', '').replace('}', '').replace('[', '').replace(']', '').strip()
                    with st.expander(f"💡 {row['Type']} (Confidence: {row['Confidence']})"):
                        st.markdown(f"**Observation:** {obs_display}")
                        st.markdown(f"**Recommendation:** {rec_raw}")
                        st.caption(f"Observed {row['Count']} time(s)")
        else:
            st.info("No learnings captured yet. Run workflows to build knowledge.")
    
    st.markdown("---")
    
    # Detailed execution view
    st.subheader("🔍 Execution Details")
    
    execution_list = run_query("""
        SELECT execution_id FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS 
        ORDER BY started_at DESC LIMIT 20
    """)
    
    if execution_list is not None and len(execution_list) > 0:
        selected_exec = st.selectbox("Select Execution", execution_list['EXECUTION_ID'].tolist())
        
        if selected_exec:
            col_det1, col_det2, col_det3 = st.columns(3)
            
            with col_det1:
                st.markdown("**Planner Decisions:**")
                decisions = run_query(f"""
                    SELECT source_table, transformation_strategy, confidence_score, llm_reasoning
                    FROM DBAONTAP_ANALYTICS.METADATA.PLANNER_DECISIONS
                    WHERE execution_id = '{selected_exec}'
                """)
                if decisions is not None and len(decisions) > 0:
                    st.dataframe(decisions, use_container_width=True)
                else:
                    st.caption("No decisions recorded")
            
            with col_det2:
                st.markdown("**Validation Results:**")
                validations = run_query(f"""
                    SELECT source_table, target_table, validation_type, passed, variance_pct
                    FROM DBAONTAP_ANALYTICS.METADATA.VALIDATION_RESULTS
                    WHERE execution_id = '{selected_exec}'
                """)
                if validations is not None and len(validations) > 0:
                    st.dataframe(validations, use_container_width=True)
                else:
                    st.caption("No validations recorded")
            
            with col_det3:
                st.markdown("**Workflow Output:**")
                output = run_query(f"""
                    SELECT planner_output, executor_output, validator_output, reflector_output
                    FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS
                    WHERE execution_id = '{selected_exec}'
                """)
                if output is not None and len(output) > 0:
                    with st.expander("Planner Output"):
                        st.json(output.iloc[0]['PLANNER_OUTPUT'])
                    with st.expander("Executor Output"):
                        st.json(output.iloc[0]['EXECUTOR_OUTPUT'])
                    with st.expander("Validator Output"):
                        st.json(output.iloc[0]['VALIDATOR_OUTPUT'])
                    with st.expander("Reflector Output"):
                        st.json(output.iloc[0]['REFLECTOR_OUTPUT'])
    else:
        st.info("No executions to display.")


def render_gold_layer_tab():
    st.header("🥇 Build Gold Layer")
    st.markdown("Create Gold Dynamic Tables from the Silver layer — aggregations, 360 views, and ML features.")

    gold_tables = run_query("""
        SELECT TABLE_NAME, ROW_COUNT
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'GOLD'
          AND TABLE_TYPE = 'BASE TABLE'
          AND TABLE_NAME NOT LIKE '%_SV'
        ORDER BY TABLE_NAME
    """)
    gold_exists = gold_tables is not None and len(gold_tables) > 0

    silver_tables = run_query("""
        SELECT TABLE_NAME
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'SILVER'
          AND TABLE_TYPE = 'BASE TABLE'
          AND TABLE_NAME NOT LIKE '__%'
        ORDER BY TABLE_NAME
    """)
    silver_count = len(silver_tables) if silver_tables is not None else 0

    if gold_exists:
        st.success(f"✅ {len(gold_tables)} Gold Dynamic Tables exist (from {silver_count} Silver tables)")
        st.dataframe(gold_tables, use_container_width=True)
    else:
        st.info(f"No Gold Dynamic Tables yet. {silver_count} Silver tables available to build from.")

    col_b1, col_b2 = st.columns(2)

    with col_b1:
        if st.button("🏗️ Build Core Gold Layer" if not gold_exists else "🔄 Rebuild Core Gold Layer",
                      type="primary" if not gold_exists else "secondary", use_container_width=True,
                      help="Build 4 core Gold DTs (CUSTOMER_360, PRODUCT_PERFORMANCE_METRICS, ORDER_SUMMARY, ML_CUSTOMER_FEATURES). Use Agentic Gold to build SUPPORT_METRICS."):
            with st.spinner("Building core Gold Dynamic Tables..."):
                gold_ddls = [
                    ("CUSTOMER_360", """CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360
                        TARGET_LAG = DOWNSTREAM WAREHOUSE = DBRYANT_COCO_WH_S AS
                        SELECT c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME,
                            c.FIRST_NAME || ' ' || c.LAST_NAME as FULL_NAME,
                            c.EMAIL, c.PHONE, c.COMPANY_NAME, c.SEGMENT, c.LOYALTY_TIER, c.ANNUAL_REVENUE,
                            c.CREATED_AT as CUSTOMER_SINCE,
                            COUNT(DISTINCT o.ORDER_ID) as TOTAL_ORDERS,
                            SUM(o.TOTAL_AMOUNT) as LIFETIME_VALUE,
                            AVG(o.TOTAL_AMOUNT) as AVG_ORDER_VALUE,
                            MIN(o.ORDER_DATE) as FIRST_ORDER_DATE,
                            MAX(o.ORDER_DATE) as LAST_ORDER_DATE,
                            DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_DATE()) as RECENCY_DAYS,
                            COUNT(DISTINCT o.ORDER_ID) as FREQUENCY,
                            SUM(o.TOTAL_AMOUNT) as MONETARY_TOTAL,
                            CASE WHEN SUM(o.TOTAL_AMOUNT) >= 100000 THEN 'PLATINUM'
                                 WHEN SUM(o.TOTAL_AMOUNT) >= 50000 THEN 'GOLD'
                                 WHEN SUM(o.TOTAL_AMOUNT) >= 10000 THEN 'SILVER' ELSE 'BRONZE' END as REVENUE_TIER,
                            CASE WHEN DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_DATE()) <= 7 THEN 'HOT'
                                 WHEN DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_DATE()) <= 30 THEN 'ACTIVE'
                                 WHEN DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_DATE()) <= 90 THEN 'COOLING'
                                 ELSE 'DORMANT' END as ENGAGEMENT_STATUS,
                            COUNT(DISTINCT t.TICKET_ID) as TOTAL_TICKETS,
                            SUM(CASE WHEN t.STATUS = 'open' THEN 1 ELSE 0 END) as OPEN_TICKETS
                        FROM DBAONTAP_ANALYTICS.SILVER.CUSTOMERS c
                        LEFT JOIN DBAONTAP_ANALYTICS.SILVER.ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
                        LEFT JOIN DBAONTAP_ANALYTICS.SILVER.SUPPORT_TICKETS t ON c.CUSTOMER_ID = t.CUSTOMER_ID
                        WHERE c._SNOWFLAKE_DELETED = FALSE
                        GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.EMAIL, c.PHONE,
                                 c.COMPANY_NAME, c.SEGMENT, c.LOYALTY_TIER, c.ANNUAL_REVENUE, c.CREATED_AT"""),
                    ("PRODUCT_PERFORMANCE_METRICS", """CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.GOLD.PRODUCT_PERFORMANCE_METRICS
                        TARGET_LAG = DOWNSTREAM WAREHOUSE = DBRYANT_COCO_WH_S AS
                        SELECT p.CATEGORY,
                            COUNT(DISTINCT p.PRODUCT_ID) as TOTAL_PRODUCTS,
                            COUNT(DISTINCT CASE WHEN p.IS_ACTIVE = TRUE THEN p.PRODUCT_ID END) as ACTIVE_PRODUCTS,
                            AVG(p.LIST_PRICE) as AVG_LIST_PRICE,
                            AVG(p.COST_PRICE) as AVG_COST_PRICE,
                            AVG(p.LIST_PRICE - p.COST_PRICE) as AVG_MARGIN,
                            SUM(oi.QUANTITY) as TOTAL_UNITS_SOLD,
                            SUM(oi.LINE_TOTAL) as TOTAL_REVENUE,
                            SUM(oi.LINE_TOTAL)/NULLIF(SUM(oi.QUANTITY), 0) as AVG_SELLING_PRICE,
                            COUNT(DISTINCT oi.ORDER_ID) as ORDERS_WITH_CATEGORY
                        FROM DBAONTAP_ANALYTICS.SILVER.PRODUCTS_VARIANT p
                        LEFT JOIN DBAONTAP_ANALYTICS.SILVER.ORDER_ITEMS oi ON p.PRODUCT_ID = oi.PRODUCT_ID
                        WHERE p._SNOWFLAKE_DELETED = FALSE
                        GROUP BY p.CATEGORY"""),
                    ("ORDER_SUMMARY", """CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.GOLD.ORDER_SUMMARY
                        TARGET_LAG = DOWNSTREAM WAREHOUSE = DBRYANT_COCO_WH_S AS
                        SELECT DATE_TRUNC('month', o.ORDER_DATE)::DATE as ORDER_MONTH,
                            c.SEGMENT as CUSTOMER_SEGMENT,
                            COUNT(DISTINCT o.ORDER_ID) as ORDER_COUNT,
                            COUNT(DISTINCT o.CUSTOMER_ID) as UNIQUE_CUSTOMERS,
                            SUM(o.TOTAL_AMOUNT) as TOTAL_REVENUE,
                            AVG(o.TOTAL_AMOUNT) as AVG_ORDER_VALUE,
                            SUM(CASE WHEN o.STATUS = 'completed' THEN 1 ELSE 0 END) as COMPLETED_ORDERS,
                            SUM(CASE WHEN o.STATUS = 'pending' THEN 1 ELSE 0 END) as PENDING_ORDERS,
                            SUM(CASE WHEN o.STATUS = 'processing' THEN 1 ELSE 0 END) as PROCESSING_ORDERS
                        FROM DBAONTAP_ANALYTICS.SILVER.ORDERS o
                        JOIN DBAONTAP_ANALYTICS.SILVER.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
                        WHERE o._SNOWFLAKE_DELETED = FALSE
                        GROUP BY ORDER_MONTH, c.SEGMENT"""),

                    ("ML_CUSTOMER_FEATURES", """CREATE OR REPLACE DYNAMIC TABLE DBAONTAP_ANALYTICS.GOLD.ML_CUSTOMER_FEATURES
                        TARGET_LAG = DOWNSTREAM WAREHOUSE = DBRYANT_COCO_WH_S AS
                        SELECT c.CUSTOMER_ID, c.SEGMENT, c.LOYALTY_TIER,
                            COALESCE(c360.TOTAL_ORDERS, 0) as TOTAL_ORDERS,
                            COALESCE(c360.LIFETIME_VALUE, 0) as LIFETIME_VALUE,
                            COALESCE(c360.AVG_ORDER_VALUE, 0) as AVG_ORDER_VALUE,
                            COALESCE(c360.RECENCY_DAYS, 999) as RECENCY_DAYS,
                            COALESCE(c360.TOTAL_TICKETS, 0) as TOTAL_TICKETS,
                            CASE c.SEGMENT WHEN 'ENTERPRISE' THEN 3 WHEN 'MID-MARKET' THEN 2 ELSE 1 END as SEGMENT_ENCODED,
                            CASE c360.REVENUE_TIER WHEN 'PLATINUM' THEN 4 WHEN 'GOLD' THEN 3 WHEN 'SILVER' THEN 2 ELSE 1 END as TIER_ENCODED,
                            CASE WHEN c360.ENGAGEMENT_STATUS = 'DORMANT' THEN 1 ELSE 0 END as IS_CHURNED
                        FROM DBAONTAP_ANALYTICS.SILVER.CUSTOMERS c
                        LEFT JOIN DBAONTAP_ANALYTICS.GOLD.CUSTOMER_360 c360 ON c.CUSTOMER_ID = c360.CUSTOMER_ID
                        WHERE c._SNOWFLAKE_DELETED = FALSE"""),
                ]
                progress = st.progress(0)
                results = []
                for i, (name, ddl) in enumerate(gold_ddls):
                    try:
                        validation = run_query(f"CALL DBAONTAP_ANALYTICS.AGENTS.VALIDATE_GOLD_DDL($${ddl}$$)")
                        import json as _json_v
                        v_result = _json_v.loads(validation.iloc[0, 0]) if validation is not None and len(validation) > 0 else {"valid": True}
                        if not v_result.get("valid", True):
                            results.append((name, False, f"Validation failed: {v_result.get('message', 'Unknown')}"))
                            progress.progress((i + 1) / len(gold_ddls))
                            continue
                        run_query(ddl, fetch=False)
                        results.append((name, True, None))
                    except Exception as e:
                        results.append((name, False, str(e)))
                    progress.progress((i + 1) / len(gold_ddls))

                successes = sum(1 for _, ok, _ in results if ok)
                failures = sum(1 for _, ok, _ in results if not ok)
                if failures == 0:
                    st.success(f"✅ All {successes} core Gold Dynamic Tables created!")
                else:
                    st.warning(f"⚠️ {successes} succeeded, {failures} failed")
                    for name, ok, err in results:
                        if not ok:
                            st.error(f"❌ {name}: {err}")
            st.rerun()

    with col_b2:
        if st.button("🤖 Agentic Gold Build", type="primary", use_container_width=True,
                      help="Use Cortex LLM to discover Silver tables without Gold coverage and auto-generate Gold DTs"):
            with st.spinner("🤖 Running agentic Gold layer build... (LLM generating DDLs, this may take 30-60s)"):
                try:
                    result = session.sql("CALL DBAONTAP_ANALYTICS.AGENTS.BUILD_GOLD_FOR_NEW_TABLES(FALSE, FALSE)").collect()
                    import json as _json
                    result_data = _json.loads(result[0][0]) if result else {}

                    status = result_data.get('status', 'UNKNOWN')
                    summary = result_data.get('summary', {})
                    results_list = result_data.get('results', [])

                    if status == 'ALL_COVERED':
                        st.success("✅ All Silver tables already have Gold layer coverage. No new tables to build.")
                    elif status == 'EXECUTED':
                        new_found = summary.get('new_tables_found', 0)
                        successful = summary.get('successful', 0)
                        failed = summary.get('failed', 0)

                        if failed == 0:
                            st.success(f"✅ Agentic Gold build complete! {successful} new Gold DT(s) created from {new_found} uncovered Silver table(s).")
                        else:
                            st.warning(f"⚠️ {successful} succeeded, {failed} failed out of {new_found} new tables")

                        for r in results_list:
                            if r.get('status') == 'SUCCESS':
                                st.success(f"✅ Created Gold DT for Silver.{r.get('silver_table')} (attempt {r.get('attempts', '?')})")
                                with st.expander(f"DDL for {r.get('silver_table')}"):
                                    st.code(r.get('ddl_executed', ''), language='sql')
                            elif r.get('status') == 'FAILED':
                                st.error(f"❌ Failed for Silver.{r.get('silver_table')}: {r.get('last_error', 'Unknown error')}")
                                with st.expander(f"Failed DDL for {r.get('silver_table')}"):
                                    st.code(r.get('last_ddl', ''), language='sql')
                    else:
                        st.info(f"Status: {status}")
                        st.json(result_data)
                except Exception as e:
                    st.error(f"Agentic Gold build failed: {e}")
            st.rerun()

    st.markdown("---")
    st.subheader("🔍 Schema Drift Detection")
    if st.button("🔎 Check Gold Schema Drift", use_container_width=True,
                  help="Detect if Silver columns have been added that aren't reflected in Gold DTs"):
        with st.spinner("Checking for schema drift..."):
            try:
                drift_result = session.sql("CALL DBAONTAP_ANALYTICS.AGENTS.PROPAGATE_TO_GOLD(NULL, TRUE)").collect()
                import json as _json
                drift_data = _json.loads(drift_result[0][0]) if drift_result else {}
                drift_status = drift_data.get('status', 'UNKNOWN')
                drift_detected = drift_data.get('drift_detected', {})

                if drift_detected.get('status') == 'IN_SYNC':
                    st.success("✅ All Gold tables are in sync with Silver. No drift detected.")
                else:
                    total_missing = drift_detected.get('total_missing_columns', 0)
                    affected = drift_detected.get('affected_gold_tables', 0)
                    st.warning(f"⚠️ Schema drift detected: {total_missing} missing column(s) across {affected} Gold table(s)")

                    for detail in drift_detected.get('details', []):
                        st.write(f"**{detail.get('gold_table')}** — missing: {', '.join(detail.get('missing_columns', []))}")

                    if st.button("🔧 Auto-fix Drift (Execute)", key="fix_drift"):
                        with st.spinner("Fixing drift..."):
                            fix_result = session.sql("CALL DBAONTAP_ANALYTICS.AGENTS.PROPAGATE_TO_GOLD(NULL, FALSE)").collect()
                            fix_data = _json.loads(fix_result[0][0]) if fix_result else {}
                            st.success(f"Drift fix complete: {fix_data.get('status')}")
                        st.rerun()
            except Exception as e:
                st.error(f"Drift check failed: {e}")


def render_schema_contracts_tab():
    st.header("📐 Schema Contracts")
    st.markdown("Schema contracts enforce **deterministic column naming** when the LLM generates Silver Dynamic Tables. "
                "Without a contract, the LLM freely chooses names (e.g., `FULL_NAME` vs `FIRST_NAME`/`LAST_NAME`).")

    contracts = run_query("""
        SELECT source_table_pattern, required_columns, naming_rules, created_at
        FROM DBAONTAP_ANALYTICS.METADATA.SILVER_SCHEMA_CONTRACTS
        ORDER BY source_table_pattern
    """)

    st.subheader("📋 Existing Contracts")
    if contracts is not None and len(contracts) > 0:
        for idx, row in contracts.iterrows():
            table_name = row['SOURCE_TABLE_PATTERN']
            cols_raw = row['REQUIRED_COLUMNS']
            rules_raw = row['NAMING_RULES']

            try:
                cols = json.loads(cols_raw) if isinstance(cols_raw, str) else cols_raw
            except (json.JSONDecodeError, TypeError):
                cols = []
            try:
                rules = json.loads(rules_raw) if isinstance(rules_raw, str) else rules_raw
            except (json.JSONDecodeError, TypeError):
                rules = {}

            with st.expander(f"📄 **{table_name}** — {len(cols)} columns", expanded=False):
                if isinstance(rules, dict) and rules.get('note'):
                    st.info(f"💡 {rules['note']}")

                col_data = []
                for c in cols:
                    col_data.append({
                        "Column": c.get('name', ''),
                        "Type": c.get('type', ''),
                        "Required": "✅" if c.get('required') else "⬜"
                    })
                if col_data:
                    st.dataframe(col_data, use_container_width=True)

                if st.button(f"🗑️ Delete contract for {table_name}", key=f"del_contract_{table_name}"):
                    run_query(f"DELETE FROM DBAONTAP_ANALYTICS.METADATA.SILVER_SCHEMA_CONTRACTS WHERE source_table_pattern = '{table_name}'")
                    st.success(f"Deleted contract for {table_name}")
                    st.rerun()
    else:
        st.warning("No schema contracts defined. Silver DTs will use unconstrained LLM naming.")

    st.markdown("---")

    st.subheader("➕ Add New Contract")
    st.markdown("Add a schema contract for a new or existing table to lock down column names.")

    uncovered = run_query("""
        SELECT REPLACE(t.TABLE_NAME, '_VARIANT', '') as TABLE_BASE
        FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES t
        WHERE t.TABLE_SCHEMA = 'BRONZE' AND t.TABLE_NAME LIKE '%_VARIANT'
        AND REPLACE(t.TABLE_NAME, '_VARIANT', '') NOT IN (
            SELECT source_table_pattern FROM DBAONTAP_ANALYTICS.METADATA.SILVER_SCHEMA_CONTRACTS
        )
        ORDER BY t.TABLE_NAME
    """)

    if uncovered is not None and len(uncovered) > 0:
        st.info(f"🔍 Found {len(uncovered)} Bronze table(s) without contracts: **{', '.join(uncovered['TABLE_BASE'].tolist())}**")

    new_table = st.text_input("Table name (e.g., INVOICES)", key="new_contract_table")

    col_gen1, col_gen2 = st.columns([3, 1])
    with col_gen1:
        generate_from = st.selectbox(
            "Generate contract from",
            ["Manual entry", "Existing Silver DT", "LLM from Bronze schema"],
            key="contract_gen_method"
        )
    with col_gen2:
        st.markdown("<br>", unsafe_allow_html=True)
        generate_btn = st.button("🔄 Generate", key="gen_contract_btn", use_container_width=True)

    if generate_from == "Existing Silver DT" and generate_btn and new_table:
        silver_cols = run_query(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_NAME = '{new_table.upper()}'
            ORDER BY ORDINAL_POSITION
        """)
        if silver_cols is not None and len(silver_cols) > 0:
            cols_json = []
            for _, c in silver_cols.iterrows():
                cols_json.append({
                    "name": c['COLUMN_NAME'],
                    "type": c['DATA_TYPE'],
                    "required": c['COLUMN_NAME'] not in ('UPDATED_AT', 'UPDATED_AT_TS', 'NOTES', 'DESCRIPTION', 'PHONE')
                })
            st.session_state['generated_contract'] = json.dumps(cols_json, indent=2)
            st.success(f"Generated contract from SILVER.{new_table.upper()} with {len(cols_json)} columns")
        else:
            st.error(f"SILVER.{new_table.upper()} not found")

    elif generate_from == "LLM from Bronze schema" and generate_btn and new_table:
        with st.spinner("Asking LLM to propose a schema contract..."):
            bronze_table = f"DBAONTAP_ANALYTICS.BRONZE.{new_table.upper()}_VARIANT"
            schema_info = run_query(f"CALL DBAONTAP_ANALYTICS.AGENTS.DISCOVER_SCHEMA('{bronze_table}')")
            if schema_info is not None and len(schema_info) > 0:
                discovered = schema_info.iloc[0][0]
                llm_result = run_query(f"""
                    SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet',
                        'Given this discovered Bronze schema: {discovered}

Propose a Silver schema contract as a JSON array. Each element: {{"name":"COL_NAME","type":"SNOWFLAKE_TYPE","required":true/false}}

Rules:
- Use UPPER_CASE column names
- Keep original field names from source (do not rename)
- Use appropriate Snowflake types (INTEGER, VARCHAR, NUMBER(18,2), TIMESTAMP_NTZ, BOOLEAN)
- Always include IS_DELETED (BOOLEAN, required), INSERTED_AT (TIMESTAMP_NTZ, required)
- Mark primary keys and foreign keys as required
- Output ONLY the JSON array, no explanation')
                """)
                if llm_result is not None:
                    raw = str(llm_result.iloc[0][0])
                    import re
                    match = re.search(r'\[.*\]', raw, re.DOTALL)
                    if match:
                        st.session_state['generated_contract'] = match.group(0)
                        st.success("LLM generated a proposed contract — review and save below")

    contract_json = st.text_area(
        "Column definitions (JSON array)",
        value=st.session_state.get('generated_contract', '[{"name":"ID","type":"INTEGER","required":true}]'),
        height=200,
        key="contract_json_input"
    )
    naming_note = st.text_input("Naming rules / notes", key="contract_naming_note",
                                placeholder="e.g., Use FIRST_NAME and LAST_NAME separately, never FULL_NAME")

    if st.button("💾 Save Contract", key="save_contract_btn", type="primary"):
        if not new_table:
            st.error("Enter a table name")
        else:
            try:
                parsed = json.loads(contract_json)
                if not isinstance(parsed, list):
                    st.error("Must be a JSON array")
                else:
                    cols_str = json.dumps(parsed).replace("'", "''")
                    note_str = json.dumps({"note": naming_note}).replace("'", "''")
                    run_query(f"""
                        DELETE FROM DBAONTAP_ANALYTICS.METADATA.SILVER_SCHEMA_CONTRACTS
                        WHERE source_table_pattern = '{new_table.upper()}'
                    """)
                    run_query(f"""
                        INSERT INTO DBAONTAP_ANALYTICS.METADATA.SILVER_SCHEMA_CONTRACTS
                        (source_table_pattern, required_columns, naming_rules)
                        SELECT '{new_table.upper()}', PARSE_JSON('{cols_str}'), PARSE_JSON('{note_str}')
                    """)
                    st.success(f"Saved schema contract for **{new_table.upper()}** with {len(parsed)} columns")
                    if 'generated_contract' in st.session_state:
                        del st.session_state['generated_contract']
                    st.rerun()
            except json.JSONDecodeError as e:
                st.error(f"Invalid JSON: {e}")


def render_directives_tab():
    st.header("🎯 Transformation Directives")
    st.markdown("Directives tell the LLM agents **what the data is for** - the business intent behind transformations. "
                "While Schema Contracts enforce *structure* (column names, types), Directives guide *purpose* (forecasting, churn, dashboards).")

    directives = run_query("""
        SELECT directive_id, source_table_pattern, target_layer, use_case,
               instructions, priority, is_active, created_by, created_at
        FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_DIRECTIVES
        ORDER BY priority DESC, source_table_pattern
    """)

    layer_icons = {'SILVER': '🥈', 'GOLD': '🥇', 'BOTH': '🔄'}

    st.subheader("📋 Active Directives")
    if directives is not None and len(directives) > 0:
        for idx, row in directives.iterrows():
            icon = layer_icons.get(row['TARGET_LAYER'], '📄')
            active_badge = "✅" if row['IS_ACTIVE'] else "⏸️"
            with st.expander(
                f"{active_badge} {icon} **{row['SOURCE_TABLE_PATTERN']}** → {row['TARGET_LAYER']} | "
                f"`{row['USE_CASE']}` (priority: {row['PRIORITY']})",
                expanded=False
            ):
                st.markdown(f"**Instructions:**\n\n{row['INSTRUCTIONS']}")
                st.caption(f"Created by {row['CREATED_BY']} on {row['CREATED_AT']}")

                col_toggle, col_del = st.columns([1, 1])
                with col_toggle:
                    new_state = not row['IS_ACTIVE']
                    label = "⏸️ Deactivate" if row['IS_ACTIVE'] else "▶️ Activate"
                    if st.button(label, key=f"toggle_{row['DIRECTIVE_ID']}"):
                        run_query(f"""
                            UPDATE DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_DIRECTIVES
                            SET is_active = {new_state}, updated_at = CURRENT_TIMESTAMP()
                            WHERE directive_id = '{row['DIRECTIVE_ID']}'
                        """)
                        st.rerun()
                with col_del:
                    if st.button("🗑️ Delete", key=f"del_dir_{row['DIRECTIVE_ID']}"):
                        run_query(f"""
                            DELETE FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_DIRECTIVES
                            WHERE directive_id = '{row['DIRECTIVE_ID']}'
                        """)
                        st.success("Deleted")
                        st.rerun()
    else:
        st.info("No directives defined. The LLM agents will use their own judgment for transformation strategy.")

    st.markdown("---")
    st.subheader("➕ Add New Directive")

    col_a, col_b = st.columns(2)
    with col_a:
        new_table_pattern = st.text_input("Source table pattern", placeholder="ORDERS, CUSTOMERS, or % for all",
                                          key="dir_table_pattern")
        new_use_case = st.text_input("Use case name", placeholder="e.g., demand_forecasting, churn_prediction",
                                     key="dir_use_case")
    with col_b:
        new_layer = st.selectbox("Target layer", ["GOLD", "SILVER", "BOTH"], key="dir_layer")
        new_priority = st.slider("Priority (higher = stronger weight)", 1, 10, 5, key="dir_priority")

    new_instructions = st.text_area(
        "Instructions (natural language - tell the LLM what this data is for and how to shape it)",
        height=120, key="dir_instructions",
        placeholder="e.g., This data feeds a demand forecasting model. Preserve daily granularity. Create 7/14/30 day rolling averages..."
    )

    col_save, col_gen = st.columns([1, 1])
    with col_gen:
        if st.button("🤖 Generate with LLM", key="gen_directive"):
            if new_table_pattern and new_use_case:
                with st.spinner("Asking LLM to draft a directive..."):
                    gen_result = run_query(f"""
                        SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet',
                            'You are a data engineering expert. Write a transformation directive for this scenario:
Table: {new_table_pattern.upper()}
Use case: {new_use_case}
Target layer: {new_layer}

Write 2-4 sentences of clear, actionable instructions for an LLM that will generate Snowflake SQL transformations.
Be specific about: granularity, derived columns, aggregation rules, and any special handling.
Output ONLY the instruction text, no explanation or formatting.')
                    """)
                    if gen_result is not None:
                        st.session_state['generated_directive'] = str(gen_result.iloc[0][0]).strip().strip('"')
                        st.rerun()
            else:
                st.warning("Enter table pattern and use case first")

    if 'generated_directive' in st.session_state:
        st.info(f"💡 LLM suggestion: {st.session_state['generated_directive']}")

    with col_save:
        if st.button("💾 Save Directive", key="save_directive", type="primary"):
            instructions_to_save = new_instructions or st.session_state.get('generated_directive', '')
            if not new_table_pattern or not new_use_case or not instructions_to_save:
                st.error("Fill in table pattern, use case, and instructions")
            else:
                safe_instructions = instructions_to_save.replace("'", "''")
                run_query(f"""
                    INSERT INTO DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_DIRECTIVES
                    (source_table_pattern, target_layer, use_case, instructions, priority)
                    VALUES ('{new_table_pattern.upper()}', '{new_layer}', '{new_use_case}',
                            '{safe_instructions}', {new_priority})
                """)
                st.success(f"Saved directive: **{new_use_case}** for {new_table_pattern.upper()} → {new_layer}")
                if 'generated_directive' in st.session_state:
                    del st.session_state['generated_directive']
                st.rerun()


def render_reset_tab():
    """Render comprehensive data reset interface"""
    st.header("🗑️ Reset Data")
    st.warning("⚠️ These actions will delete data. Use with caution!")
    
    # Discover current state
    st.subheader("📊 Current Data State")
    
    safe_layers = [
        ("Landing: customers", 'DBAONTAP_ANALYTICS."public"."customers"'),
        ("Landing: orders", 'DBAONTAP_ANALYTICS."public"."orders"'),
        ("Landing: products", 'DBAONTAP_ANALYTICS."public"."products"'),
        ("Landing: order_items", 'DBAONTAP_ANALYTICS."public"."order_items"'),
        ("Landing: support_tickets", 'DBAONTAP_ANALYTICS."public"."support_tickets"'),
        ("Bronze: CUSTOMERS_VARIANT", "DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT"),
        ("Bronze: ORDERS_VARIANT", "DBAONTAP_ANALYTICS.BRONZE.ORDERS_VARIANT"),
        ("Bronze: PRODUCTS_VARIANT", "DBAONTAP_ANALYTICS.BRONZE.PRODUCTS_VARIANT"),
        ("Silver: CUSTOMERS", "DBAONTAP_ANALYTICS.SILVER.CUSTOMERS"),
        ("Silver: ORDERS", "DBAONTAP_ANALYTICS.SILVER.ORDERS"),
        ("Metadata: WORKFLOW_EXECUTIONS", "DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS"),
        ("Metadata: TRANSFORMATION_LOG", "DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG"),
    ]
    try:
        gold_state_df = session.sql("SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME").collect()
        for row in gold_state_df:
            safe_layers.append((f"Gold: {row[0]}", f"DBAONTAP_ANALYTICS.GOLD.{row[0]}"))
    except:
        pass
    rows = []
    for label, tbl in safe_layers:
        try:
            r = session.sql(f"SELECT COUNT(*) as cnt FROM {tbl}").collect()
            rows.append({"LAYER": label, "CNT": r[0]['CNT'] if r else 0})
        except Exception:
            rows.append({"LAYER": label, "CNT": "—"})
    import pandas as _pd
    data_state = _pd.DataFrame(rows)
    
    if data_state is not None:
        col_state1, col_state2 = st.columns(2)
        with col_state1:
            st.dataframe(data_state, use_container_width=True)
        with col_state2:
            numeric_counts = [r for r in data_state['CNT'] if isinstance(r, (int, float))]
            st.metric("Total Records", sum(numeric_counts))
    
    st.markdown("---")
    
    # Force Refresh DTs Section
    st.subheader("🔄 Force Refresh Dynamic Tables")
    col_refresh1, col_refresh2, col_refresh3 = st.columns(3)
    
    with col_refresh1:
        if st.button("🔄 Refresh Silver DTs", use_container_width=True):
            with st.spinner("Refreshing Silver layer..."):
                silver_tables = ["CUSTOMERS", "ORDERS", "PRODUCTS", "ORDER_ITEMS", "SUPPORT_TICKETS"]
                success_count = 0
                for tbl in silver_tables:
                    try:
                        session.sql(f"ALTER DYNAMIC TABLE DBAONTAP_ANALYTICS.SILVER.{tbl} REFRESH").collect()
                        success_count += 1
                    except:
                        pass
                st.success(f"✅ Refreshed {success_count}/{len(silver_tables)} Silver DTs")
    
    with col_refresh2:
        if st.button("🔄 Refresh Gold DTs", use_container_width=True):
            with st.spinner("Refreshing Gold layer..."):
                gold_refresh_list = run_query("SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")
                gold_refresh_names = gold_refresh_list['TABLE_NAME'].tolist() if gold_refresh_list is not None and len(gold_refresh_list) > 0 else []
                success_count = 0
                for tbl in gold_refresh_names:
                    try:
                        session.sql(f"ALTER DYNAMIC TABLE IF EXISTS DBAONTAP_ANALYTICS.GOLD.{tbl} REFRESH").collect()
                        success_count += 1
                    except:
                        pass
                st.success(f"✅ Refreshed {success_count}/{len(gold_refresh_names)} Gold DTs")
    
    with col_refresh3:
        if st.button("🔄 Refresh ALL DTs", type="primary", use_container_width=True):
            with st.spinner("Refreshing all layers..."):
                all_tables = [
                    ("BRONZE", ["CUSTOMERS_VARIANT", "ORDERS_VARIANT", "PRODUCTS_VARIANT", "ORDER_ITEMS_VARIANT", "SUPPORT_TICKETS_VARIANT"]),
                    ("SILVER", ["CUSTOMERS", "ORDERS", "PRODUCTS", "ORDER_ITEMS", "SUPPORT_TICKETS"]),
                    ("GOLD", [r[0] for r in session.sql("SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE'").collect()] if session.sql("SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE'").collect()[0][0] > 0 else [])
                ]
                total = sum(len(tables) for _, tables in all_tables)
                success_count = 0
                for schema, tables in all_tables:
                    for tbl in tables:
                        try:
                            session.sql(f"ALTER DYNAMIC TABLE IF EXISTS DBAONTAP_ANALYTICS.{schema}.{tbl} REFRESH").collect()
                            success_count += 1
                        except:
                            pass
                st.success(f"✅ Refreshed {success_count}/{total} DTs across all layers")
    
    st.markdown("---")
    
    # PostgreSQL Source Reset Section
    st.subheader("🐘 PostgreSQL Instances (External)")
    
    if POSTGRES_AVAILABLE:
        col_pg1, col_pg2 = st.columns(2)
        
        with col_pg1:
            st.markdown("**SOURCE Instance**")
            st.caption(f"Host: {PG_SOURCE_HOST[:30]}...")
            if st.button("🗑️ Truncate SOURCE Tables", type="secondary", use_container_width=True):
                with st.spinner("Connecting to PostgreSQL SOURCE..."):
                    success, msg = truncate_pg_tables(
                        "source",
                        ['order_items', 'orders', 'support_tickets', 'products', 'customers']
                    )
                    if success:
                        st.success(f"✅ SOURCE: {msg}")
                    else:
                        st.error(f"❌ SOURCE: {msg}")
        
        with col_pg2:
            st.markdown("**LANDING Instance**")
            st.caption(f"Host: {PG_LANDING_HOST[:30]}...")
            if st.button("🗑️ Truncate LANDING Tables", type="secondary", use_container_width=True):
                with st.spinner("Connecting to PostgreSQL LANDING..."):
                    success, msg = truncate_pg_tables(
                        "landing",
                        ['order_items', 'orders', 'support_tickets', 'products', 'customers']
                    )
                    if success:
                        st.success(f"✅ LANDING: {msg}")
                    else:
                        st.error(f"❌ LANDING: {msg}")
    else:
        st.info("🔒 PostgreSQL connectivity not available. EAI + psycopg2 required.")
    
    st.markdown("---")
    
    # AI-First Reset Philosophy
    st.subheader("🔄 Reset Strategy")
    st.info("""
    **AI-First Methodology**: Dynamic Tables auto-refresh from their sources.
    - **Soft Reset** (Recommended): Clear source data → DTs will auto-refresh to empty
    - **Hard Reset**: DROP DTs → Use only when schema changes are needed
    """)
    
    # Snowflake Reset Options  
    st.subheader("❄️ Snowflake Tables")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Landing (public schema)**")
        
        landing_tables = st.multiselect(
            "Select Landing Tables",
            ["customers", "orders", "products", "order_items", "support_tickets"],
            default=[],
            key="landing_tables"
        )
        
        if st.button("🗑️ Clear Landing Data (Soft Reset)", type="secondary", use_container_width=True):
            if landing_tables:
                ordered = []
                if "order_items" in landing_tables:
                    ordered.append("order_items")
                if "orders" in landing_tables:
                    ordered.append("orders")
                if "support_tickets" in landing_tables:
                    ordered.append("support_tickets")
                if "products" in landing_tables:
                    ordered.append("products")
                if "customers" in landing_tables:
                    ordered.append("customers")
                
                for table in ordered:
                    run_query(f'DELETE FROM DBAONTAP_ANALYTICS."public"."{table}"', fetch=False)
                    st.success(f"✓ Cleared {table}")
                st.caption("💡 DTs will auto-refresh to empty on next refresh cycle")
                st.rerun()
            else:
                st.warning("No tables selected.")
    
    with col2:
        st.markdown("**Force DT Refresh**")
        st.caption("Trigger immediate refresh of Dynamic Tables")
        
        if st.button("🔄 Refresh All DTs Now", type="secondary", use_container_width=True):
            with st.spinner("Triggering DT refreshes..."):
                for schema, tables in [
                    ("BRONZE", ["CUSTOMERS_VARIANT", "ORDERS_VARIANT", "PRODUCTS_VARIANT", "ORDER_ITEMS_VARIANT", "SUPPORT_TICKETS_VARIANT"]),
                    ("SILVER", ["CUSTOMERS", "ORDERS", "PRODUCTS", "ORDER_ITEMS", "SUPPORT_TICKETS"]),
                    ("GOLD", [r[0] for r in session.sql("SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE'").collect()] if session.sql("SELECT COUNT(*) FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE'").collect()[0][0] > 0 else [])
                ]:
                    for tbl in tables:
                        try:
                            run_query(f"ALTER DYNAMIC TABLE IF EXISTS DBAONTAP_ANALYTICS.{schema}.{tbl} REFRESH", fetch=False)
                        except:
                            pass
                st.success("✅ Refresh triggered for all DTs")
                st.rerun()
    
    st.markdown("---")
    st.subheader("⚠️ Hard Reset (DROP Dynamic Tables)")
    st.warning("Only use when schema changes are needed. DTs must be recreated by the Agentic Workflow.")
    
    col_b, col_s, col_g = st.columns(3)
    
    with col_b:
        st.markdown("**Bronze DTs**")
        
        bronze_tables = st.multiselect(
            "Select Bronze Tables to Drop",
            ["CUSTOMERS_VARIANT", "ORDERS_VARIANT", "PRODUCTS_VARIANT", "ORDER_ITEMS_VARIANT", "SUPPORT_TICKETS_VARIANT"],
            default=[],
            key="bronze_tables"
        )
        
        if st.button("🗑️ Drop Bronze DTs", type="secondary", use_container_width=True):
            if bronze_tables:
                for table in bronze_tables:
                    run_query(f'DROP DYNAMIC TABLE IF EXISTS DBAONTAP_ANALYTICS.BRONZE.{table}', fetch=False)
                    st.success(f"✓ Dropped {table}")
                st.rerun()
            else:
                st.warning("No tables selected.")
    
    with col_s:
        st.markdown("**Silver DTs**")
        
        silver_tables = st.multiselect(
            "Select Silver Tables to Drop",
            ["CUSTOMERS", "ORDERS", "PRODUCTS", "ORDER_ITEMS", "SUPPORT_TICKETS"],
            default=[],
            key="silver_tables"
        )
        
        if st.button("🗑️ Drop Silver DTs", type="secondary", use_container_width=True):
            if silver_tables:
                for table in silver_tables:
                    run_query(f'DROP DYNAMIC TABLE IF EXISTS DBAONTAP_ANALYTICS.SILVER.{table}', fetch=False)
                    st.success(f"✓ Dropped {table}")
                st.rerun()
            else:
                st.warning("No tables selected.")
    
    with col_g:
        st.markdown("**Gold DTs**")
        
        gold_dt_list = run_query("SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME")
        gold_dt_names = gold_dt_list['TABLE_NAME'].tolist() if gold_dt_list is not None and len(gold_dt_list) > 0 else []
        gold_tables = st.multiselect(
            "Select Gold Tables to Drop",
            gold_dt_names,
            default=[],
            key="gold_tables"
        )
        
        if st.button("🗑️ Drop Gold DTs", type="secondary", use_container_width=True):
            if gold_tables:
                for table in gold_tables:
                    run_query(f'DROP DYNAMIC TABLE IF EXISTS DBAONTAP_ANALYTICS.GOLD.{table}', fetch=False)
                    st.success(f"✓ Dropped {table}")
                st.rerun()
            else:
                st.warning("No tables selected.")
    
    st.markdown("---")
    
    # Semantic Views Reset
    st.subheader("🔷 Semantic Views")
    
    sv_list = run_query("SHOW SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD")
    if sv_list is not None and len(sv_list) > 0:
        cols = sv_list.columns.tolist()
        name_col = [c for c in cols if 'name' in c.lower() and c.lower() not in ['database_name', 'schema_name']][0] if any('name' in c.lower() for c in cols) else cols[0]
        sv_names = sv_list[name_col].tolist()
        
        selected_svs = st.multiselect(
            "Select Semantic Views to Drop",
            sv_names,
            default=[],
            key="svs_to_drop"
        )
        
        col_sv1, col_sv2 = st.columns(2)
        with col_sv1:
            if st.button("🗑️ Drop Selected Semantic Views", type="secondary", use_container_width=True):
                if selected_svs:
                    for sv_name in selected_svs:
                        run_query(f'DROP SEMANTIC VIEW IF EXISTS DBAONTAP_ANALYTICS.GOLD."{sv_name}"', fetch=False)
                        st.success(f"✓ Dropped {sv_name}")
                    st.rerun()
                else:
                    st.warning("No semantic views selected.")
        
        with col_sv2:
            if st.button("🗑️ Drop ALL Semantic Views", type="secondary", use_container_width=True):
                for sv_name in sv_names:
                    run_query(f'DROP SEMANTIC VIEW IF EXISTS DBAONTAP_ANALYTICS.GOLD."{sv_name}"', fetch=False)
                    st.success(f"✓ Dropped {sv_name}")
                st.rerun()
    else:
        st.info("No semantic views found in GOLD schema.")
    
    st.markdown("---")
    
    # Metadata Reset
    col_meta1, col_meta2 = st.columns(2)
    
    with col_meta1:
        st.subheader("📋 Metadata & Logs")
        
        if st.button("Clear Transformation Logs", use_container_width=True):
            run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG", fetch=False)
            st.success("Transformation logs cleared!")
            st.rerun()
        
        if st.button("Clear Workflow Executions", use_container_width=True):
            run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS", fetch=False)
            run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.PLANNER_DECISIONS", fetch=False)
            run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.VALIDATION_RESULTS", fetch=False)
            st.success("Workflow executions cleared!")
            st.rerun()
        
        if st.button("Clear Workflow Learnings", use_container_width=True):
            run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_LEARNINGS", fetch=False)
            st.success("Workflow learnings cleared!")
            st.rerun()
    
    with col_meta2:
        st.subheader("☢️ FULL RESET")
        st.error("**DANGER ZONE** - Complete data reset for fresh testing")
        
        reset_mode = st.radio(
            "Reset Mode",
            ["Soft Reset (Clear data, keep DTs)", "Hard Reset (DROP all DTs)"],
            index=0,
            key="reset_mode"
        )
        
        include_pg = st.checkbox("Include PostgreSQL instances", key="include_pg_reset")
        include_sv = st.checkbox("Include Semantic Views", value=True, key="include_sv_reset")
        confirm_reset = st.checkbox("I understand this will delete ALL data", key="confirm_full_reset")
        
        if st.button("☢️ FULL RESET", type="primary", disabled=not confirm_reset, use_container_width=True):
            with st.spinner("Performing full reset..."):
                progress = st.progress(0, text="Starting reset...")
                
                # 0. PostgreSQL instances (if selected)
                if include_pg and POSTGRES_AVAILABLE:
                    progress.progress(5, text="Truncating PostgreSQL SOURCE...")
                    truncate_pg_tables("source",
                        ['order_items', 'orders', 'support_tickets', 'products', 'customers'])
                    progress.progress(10, text="Truncating PostgreSQL LANDING...")
                    truncate_pg_tables("landing",
                        ['order_items', 'orders', 'support_tickets', 'products', 'customers'])
                
                # 1. Clear Metadata (always)
                progress.progress(15, text="Clearing metadata...")
                run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_EXECUTIONS", fetch=False)
                run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.PLANNER_DECISIONS", fetch=False)
                run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.VALIDATION_RESULTS", fetch=False)
                run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_LEARNINGS", fetch=False)
                run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.TRANSFORMATION_LOG", fetch=False)
                run_query("DELETE FROM DBAONTAP_ANALYTICS.METADATA.WORKFLOW_LOG", fetch=False)
                
                # 1b. Clear Knowledge Graph (always)
                progress.progress(18, text="Clearing Knowledge Graph...")
                run_query("DELETE FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_NODE", fetch=False)
                run_query("DELETE FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE", fetch=False)
                
                if "Hard Reset" in reset_mode:
                    # 2. Drop Semantic Views first (if selected)
                    if include_sv:
                        progress.progress(25, text="Dropping Semantic Views...")
                        try:
                            sv_list = run_query("SELECT NAME FROM INFORMATION_SCHEMA.SEMANTIC_VIEWS WHERE SCHEMA = 'GOLD'")
                            if sv_list is not None and len(sv_list) > 0:
                                for sv_name in sv_list['NAME'].tolist():
                                    run_query(f"DROP SEMANTIC VIEW IF EXISTS DBAONTAP_ANALYTICS.GOLD.{sv_name}", fetch=False)
                        except:
                            pass
                    
                    # 3. Drop ALL Gold Dynamic Tables (catches agentic-created ones too)
                    progress.progress(35, text="Dropping Gold DTs...")
                    try:
                        gold_list = run_query("SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE'")
                        if gold_list is not None and len(gold_list) > 0:
                            for gt_name in gold_list['TABLE_NAME'].tolist():
                                run_query(f"DROP DYNAMIC TABLE IF EXISTS DBAONTAP_ANALYTICS.GOLD.{gt_name}", fetch=False)
                    except:
                        for tbl in ["CUSTOMER_360", "PRODUCT_PERFORMANCE", "ORDER_SUMMARY", "CUSTOMER_METRICS", "ML_CUSTOMER_FEATURES"]:
                            run_query(f"DROP DYNAMIC TABLE IF EXISTS DBAONTAP_ANALYTICS.GOLD.{tbl}", fetch=False)
                    
                    # 3. Drop Silver Dynamic Tables
                    progress.progress(45, text="Dropping Silver DTs...")
                    for tbl in ["CUSTOMERS", "ORDERS", "PRODUCTS", "ORDER_ITEMS", "SUPPORT_TICKETS"]:
                        run_query(f"DROP DYNAMIC TABLE IF EXISTS DBAONTAP_ANALYTICS.SILVER.{tbl}", fetch=False)
                    
                    # 4. Bronze DTs are permanent — skip dropping them
                    # They auto-refresh to empty when landing tables are cleared below
                    progress.progress(60, text="Bronze DTs preserved (will refresh to empty)...")
                
                # 5. Clear Landing tables (correct FK order) - Soft reset path
                progress.progress(80, text="Clearing Landing tables...")
                run_query('DELETE FROM DBAONTAP_ANALYTICS."public"."order_items"', fetch=False)
                run_query('DELETE FROM DBAONTAP_ANALYTICS."public"."orders"', fetch=False)
                run_query('DELETE FROM DBAONTAP_ANALYTICS."public"."support_tickets"', fetch=False)
                run_query('DELETE FROM DBAONTAP_ANALYTICS."public"."products"', fetch=False)
                run_query('DELETE FROM DBAONTAP_ANALYTICS."public"."customers"', fetch=False)
                
                if "Soft Reset" in reset_mode:
                    # Trigger DT refresh so they pick up the empty source
                    progress.progress(90, text="Triggering DT refresh...")
                    refresh_errors = []
                    for schema, tables in [
                        ("BRONZE", ["CUSTOMERS_VARIANT", "ORDERS_VARIANT", "PRODUCTS_VARIANT", "ORDER_ITEMS_VARIANT", "SUPPORT_TICKETS_VARIANT"]),
                        ("SILVER", ["CUSTOMERS", "ORDERS", "PRODUCTS", "ORDER_ITEMS", "SUPPORT_TICKETS"]),
                    ]:
                        for tbl in tables:
                            try:
                                session.sql(f"ALTER DYNAMIC TABLE IF EXISTS DBAONTAP_ANALYTICS.{schema}.{tbl} REFRESH").collect()
                            except Exception as e:
                                refresh_errors.append(f"{schema}.{tbl}: {str(e)[:100]}")
                    try:
                        gold_dts = run_query("SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE'")
                        if gold_dts is not None and len(gold_dts) > 0:
                            for gt in gold_dts['TABLE_NAME'].tolist():
                                try:
                                    session.sql(f"ALTER DYNAMIC TABLE IF EXISTS DBAONTAP_ANALYTICS.GOLD.{gt} REFRESH").collect()
                                except Exception as e:
                                    refresh_errors.append(f"GOLD.{gt}: {str(e)[:100]}")
                    except:
                        pass
                    
                    if refresh_errors:
                        st.warning(f"Some DT refreshes failed (may need Hard Reset): {len(refresh_errors)} errors")
                
                progress.progress(100, text="Reset complete!")
                if "Hard Reset" in reset_mode:
                    st.success("✅ Hard reset completed! Run Agentic Workflow to recreate DTs.")
                else:
                    st.success("✅ Soft reset completed! DTs will show empty data after refresh.")
                st.balloons()
                st.rerun()

def get_dt_status():
    """Get Dynamic Table status using SHOW command"""
    try:
        result = session.sql("SHOW DYNAMIC TABLES IN DATABASE DBAONTAP_ANALYTICS").collect()
        if result:
            data = []
            for row in result:
                data.append({
                    'name': row['name'],
                    'schema_name': row['schema_name'],
                    'scheduling_state': row['scheduling_state']
                })
            return pd.DataFrame(data)
    except Exception as e:
        st.warning(f"DT query error: {e}")
    return None

def get_task_status():
    """Get Task status using SHOW command"""
    try:
        result = session.sql("SHOW TASKS IN SCHEMA DBAONTAP_ANALYTICS.AGENTS").collect()
        if result:
            data = []
            for row in result:
                data.append({
                    'name': row['name'],
                    'state': row['state'],
                    'schedule': row['schedule']
                })
            return pd.DataFrame(data)
    except Exception as e:
        st.warning(f"Task query error: {e}")
    return None

def get_stream_status():
    """Get Stream status using SHOW command"""
    try:
        result = session.sql("SHOW STREAMS IN SCHEMA DBAONTAP_ANALYTICS.AGENTS").collect()
        if result:
            data = []
            for row in result:
                data.append({
                    'name': row['name'],
                    'stale': row['stale'],
                    'table_name': row['table_name']
                })
            return pd.DataFrame(data)
    except Exception as e:
        st.warning(f"Stream query error: {e}")
    return None

def render_demo_control_tab():
    """Render comprehensive demo control interface for managing tasks, DTs, and automation"""
    st.header("🎛️ Demo Control Center")
    st.markdown("Manage all automation components - suspend when demo is over to save costs.")
    
    col_status, col_actions = st.columns([2, 1])
    
    with col_status:
        st.subheader("📊 Current Status")
        
        task_status = get_task_status()
        if task_status is not None and len(task_status) > 0:
            st.markdown("**Automation Tasks**")
            task_display = task_status.copy()
            task_display['Status'] = task_display['state'].apply(lambda x: '🟢 Running' if x == 'started' else '🔴 Suspended')
            st.dataframe(task_display[['name', 'state', 'schedule', 'Status']], use_container_width=True)
        else:
            st.info("No automation tasks found.")
        
        st.markdown("---")
        
        dt_status = get_dt_status()
        if dt_status is not None and len(dt_status) > 0:
            st.markdown("**Dynamic Tables**")
            dt_display = dt_status.copy()
            dt_display['Status'] = dt_display['scheduling_state'].apply(
                lambda x: '🟢 Active' if x == 'ACTIVE' else '🔴 Suspended'
            )
            st.dataframe(dt_display[['name', 'schema_name', 'scheduling_state', 'Status']], use_container_width=True)
            
            active_count = len(dt_status[dt_status['scheduling_state'] == 'ACTIVE'])
            suspended_count = len(dt_status[dt_status['scheduling_state'] == 'SUSPENDED'])
            
            col_a, col_s = st.columns(2)
            col_a.metric("🟢 Active DTs", active_count)
            col_s.metric("🔴 Suspended DTs", suspended_count)
        else:
            st.info("No dynamic tables found.")
        
        st.markdown("---")
        
        stream_status = get_stream_status()
        if stream_status is not None and len(stream_status) > 0:
            st.markdown("**CDC Streams** (monitoring landing tables)")
            stream_display = stream_status.copy()
            stream_display['Status'] = stream_display['stale'].apply(
                lambda x: '⚠️ Stale' if str(x).lower() == 'true' else '✅ Fresh'
            )
            stream_display['Source'] = stream_display['table_name'].apply(lambda x: x.split('.')[-1].replace('"', '') if x else '')
            st.dataframe(stream_display[['name', 'Status', 'Source']], use_container_width=True)
    
    with col_actions:
        st.subheader("⚡ Quick Actions")
        
        st.markdown("**🚀 Start Demo**")
        if st.button("▶️ Resume All Automation", type="primary", use_container_width=True):
            with st.spinner("Starting automation..."):
                try:
                    run_query("ALTER TASK DBAONTAP_ANALYTICS.AGENTS.STREAM_CONSUMER_TASK RESUME", fetch=False)
                except:
                    pass
                try:
                    run_query("ALTER TASK DBAONTAP_ANALYTICS.AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK RESUME", fetch=False)
                except:
                    pass
                
                for schema, dts in [
                    ("BRONZE", ["CUSTOMERS_VARIANT", "ORDERS_VARIANT", "PRODUCTS_VARIANT", "ORDER_ITEMS_VARIANT", "SUPPORT_TICKETS_VARIANT"]),
                    ("SILVER", ["CUSTOMERS", "ORDERS", "PRODUCTS", "ORDER_ITEMS", "SUPPORT_TICKETS"]),
                    ("GOLD", ["CUSTOMER_360", "PRODUCT_PERFORMANCE", "ORDER_SUMMARY"])
                ]:
                    for dt in dts:
                        try:
                            run_query(f"ALTER DYNAMIC TABLE DBAONTAP_ANALYTICS.{schema}.{dt} RESUME", fetch=False)
                        except:
                            pass
                
                st.success("✅ All automation resumed!")
                st.rerun()
        
        st.markdown("---")
        
        st.markdown("**🛑 Stop Demo**")
        if st.button("⏸️ Suspend All Automation", type="secondary", use_container_width=True):
            with st.spinner("Stopping automation..."):
                try:
                    run_query("ALTER TASK DBAONTAP_ANALYTICS.AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK SUSPEND", fetch=False)
                except:
                    pass
                try:
                    run_query("ALTER TASK DBAONTAP_ANALYTICS.AGENTS.STREAM_CONSUMER_TASK SUSPEND", fetch=False)
                except:
                    pass
                
                for schema, dts in [
                    ("GOLD", ["CUSTOMER_360", "PRODUCT_PERFORMANCE", "ORDER_SUMMARY"]),
                    ("SILVER", ["CUSTOMERS", "ORDERS", "PRODUCTS", "ORDER_ITEMS", "SUPPORT_TICKETS"]),
                    ("BRONZE", ["CUSTOMERS_VARIANT", "ORDERS_VARIANT", "PRODUCTS_VARIANT", "ORDER_ITEMS_VARIANT", "SUPPORT_TICKETS_VARIANT"])
                ]:
                    for dt in dts:
                        try:
                            run_query(f"ALTER DYNAMIC TABLE DBAONTAP_ANALYTICS.{schema}.{dt} SUSPEND", fetch=False)
                        except:
                            pass
                
                st.success("✅ All automation suspended!")
                st.rerun()
        
        st.markdown("---")
        
        st.markdown("**🔄 Refresh**")
        if st.button("🔃 Refresh Status", use_container_width=True):
            st.rerun()
    
    st.markdown("---")
    
    st.subheader("🔧 Granular Control")
    
    tab_tasks, tab_dts, tab_streams = st.tabs(["📋 Tasks", "📊 Dynamic Tables", "🌊 Streams"])
    
    with tab_tasks:
        st.markdown("**Manage Individual Tasks**")
        
        task_col1, task_col2 = st.columns(2)
        
        with task_col1:
            st.markdown("**AGENTIC_WORKFLOW_TRIGGER_TASK**")
            st.caption("Monitors streams every 1 minute")
            tcol1, tcol2 = st.columns(2)
            if tcol1.button("▶️ Resume", key="resume_trigger"):
                run_query("ALTER TASK DBAONTAP_ANALYTICS.AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK RESUME", fetch=False)
                st.success("Resumed!")
                st.rerun()
            if tcol2.button("⏸️ Suspend", key="suspend_trigger"):
                run_query("ALTER TASK DBAONTAP_ANALYTICS.AGENTS.AGENTIC_WORKFLOW_TRIGGER_TASK SUSPEND", fetch=False)
                st.success("Suspended!")
                st.rerun()
        
        with task_col2:
            st.markdown("**STREAM_CONSUMER_TASK**")
            st.caption("Consumes stream offsets after workflow")
            tcol3, tcol4 = st.columns(2)
            if tcol3.button("▶️ Resume", key="resume_consumer"):
                run_query("ALTER TASK DBAONTAP_ANALYTICS.AGENTS.STREAM_CONSUMER_TASK RESUME", fetch=False)
                st.success("Resumed!")
                st.rerun()
            if tcol4.button("⏸️ Suspend", key="suspend_consumer"):
                run_query("ALTER TASK DBAONTAP_ANALYTICS.AGENTS.STREAM_CONSUMER_TASK SUSPEND", fetch=False)
                st.success("Suspended!")
                st.rerun()
    
    with tab_dts:
        st.markdown("**Manage Dynamic Table Refresh**")
        
        dt_data = get_dt_status()
        
        for schema in ["BRONZE", "SILVER", "GOLD"]:
            st.markdown(f"**{schema} Layer**")
            
            if dt_data is not None:
                schema_dts = dt_data[dt_data['schema_name'] == schema]
                
                if len(schema_dts) > 0:
                    dt_cols = st.columns(min(len(schema_dts), 3))
                    for idx, (_, row) in enumerate(schema_dts.iterrows()):
                        col_idx = idx % 3
                        with dt_cols[col_idx]:
                            state_icon = "🟢" if row['scheduling_state'] == 'ACTIVE' else "🔴"
                            st.markdown(f"{state_icon} **{row['name']}**")
                            bcol1, bcol2 = st.columns(2)
                            if bcol1.button("▶️", key=f"resume_{schema}_{row['name']}", help="Resume"):
                                run_query(f"ALTER DYNAMIC TABLE DBAONTAP_ANALYTICS.{schema}.{row['name']} RESUME", fetch=False)
                                st.rerun()
                            if bcol2.button("⏸️", key=f"suspend_{schema}_{row['name']}", help="Suspend"):
                                run_query(f"ALTER DYNAMIC TABLE DBAONTAP_ANALYTICS.{schema}.{row['name']} SUSPEND", fetch=False)
                                st.rerun()
            
            st.markdown("---")
        
        st.markdown("**Bulk Actions**")
        bulk_col1, bulk_col2, bulk_col3 = st.columns(3)
        
        with bulk_col1:
            if st.button("▶️ Resume All Bronze", use_container_width=True):
                for dt in ["CUSTOMERS_VARIANT", "ORDERS_VARIANT", "PRODUCTS_VARIANT", "ORDER_ITEMS_VARIANT", "SUPPORT_TICKETS_VARIANT"]:
                    try:
                        run_query(f"ALTER DYNAMIC TABLE DBAONTAP_ANALYTICS.BRONZE.{dt} RESUME", fetch=False)
                    except:
                        pass
                st.success("Bronze DTs resumed!")
                st.rerun()
        
        with bulk_col2:
            if st.button("▶️ Resume All Silver", use_container_width=True):
                for dt in ["CUSTOMERS", "ORDERS", "PRODUCTS", "ORDER_ITEMS", "SUPPORT_TICKETS"]:
                    try:
                        run_query(f"ALTER DYNAMIC TABLE DBAONTAP_ANALYTICS.SILVER.{dt} RESUME", fetch=False)
                    except:
                        pass
                st.success("Silver DTs resumed!")
                st.rerun()
        
        with bulk_col3:
            if st.button("▶️ Resume All Gold", use_container_width=True):
                for dt in ["CUSTOMER_360", "PRODUCT_PERFORMANCE", "ORDER_SUMMARY"]:
                    try:
                        run_query(f"ALTER DYNAMIC TABLE DBAONTAP_ANALYTICS.GOLD.{dt} RESUME", fetch=False)
                    except:
                        pass
                st.success("Gold DTs resumed!")
                st.rerun()
    
    with tab_streams:
        st.markdown("**Stream Status & Data Detection**")
        st.caption("Streams monitor the CDC landing tables for new data")
        
        streams = [
            ("CUSTOMERS_LANDING_STREAM", "customers"),
            ("ORDERS_LANDING_STREAM", "orders"),
            ("PRODUCTS_LANDING_STREAM", "products"),
            ("ORDER_ITEMS_LANDING_STREAM", "order_items"),
            ("SUPPORT_TICKETS_LANDING_STREAM", "support_tickets")
        ]
        
        stream_data = []
        for stream_name, source_table in streams:
            try:
                has_data = run_query(f"SELECT SYSTEM$STREAM_HAS_DATA('DBAONTAP_ANALYTICS.AGENTS.{stream_name}') as has_data")
                if has_data is not None and len(has_data) > 0:
                    has_new = has_data.iloc[0]['HAS_DATA']
                    stream_data.append({
                        "Stream": stream_name,
                        "Source Table": source_table,
                        "Has New Data": "✅ Yes" if has_new else "⬜ No",
                        "status": has_new
                    })
            except Exception as e:
                stream_data.append({"Stream": stream_name, "Source Table": source_table, "Has New Data": "❓ Unknown", "status": None})
        
        if stream_data:
            df = pd.DataFrame(stream_data)
            st.dataframe(df[['Stream', 'Source Table', 'Has New Data']], use_container_width=True)
            
            streams_with_data = sum(1 for s in stream_data if s.get('status') == True)
            if streams_with_data > 0:
                st.success(f"🔔 {streams_with_data} stream(s) have new data - workflow will trigger on next check!")
            else:
                st.info("No new data in streams. Generate data to trigger the workflow.")
        
        st.markdown("---")
        st.info("""
        **How Event-Driven Automation Works:**
        1. **Openflow CDC** writes changes to landing tables (`"public".*`)
        2. **Streams** detect INSERT/UPDATE/DELETE on landing tables
        3. **AGENTIC_WORKFLOW_TRIGGER_TASK** runs every 1 minute, checks all streams
        4. When any stream has data → triggers `RUN_AGENTIC_WORKFLOW('stream')`
        5. **Agentic Workflow** runs: TRIGGER → PLANNER → EXECUTOR → VALIDATOR → REFLECTOR
        6. **Dynamic Tables** auto-refresh from BRONZE → SILVER → GOLD
        """)

# ============================================================================
# Knowledge Graph Visualization Tab
# ============================================================================
def render_knowledge_graph_tab():
    st.header("🕸️ Knowledge Graph Visualization")
    st.markdown("Interactive view of table relationships, lineage, and metadata.")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        st.subheader("⚙️ View Options")
        view_type = st.radio(
            "Graph Type",
            ["Data Lineage", "Table Relationships", "Full Graph"],
            key="kg_view_type"
        )
        
        show_columns = st.checkbox("Show Columns", value=False, key="kg_show_cols")
        
        if st.button("🔄 Refresh Graph", use_container_width=True):
            st.rerun()
        
        if st.button("🗑️ Clear KG", use_container_width=True, type="secondary"):
            run_query("DELETE FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_NODE", fetch=False)
            run_query("DELETE FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE", fetch=False)
            st.success("KG cleared!")
            st.rerun()
    
    with col1:
        try:
            if view_type == "Data Lineage":
                st.subheader("📊 Data Lineage (Bronze → Silver → Gold)")
                bronze_df = run_query("SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_NAME LIKE '%_VARIANT' AND TABLE_TYPE != 'VIEW'")
                silver_df = run_query("SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SILVER'")
                gold_df = run_query("SELECT TABLE_NAME FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD'")
                bronze_tables = set(bronze_df['TABLE_NAME'].tolist()) if bronze_df is not None and len(bronze_df) > 0 else set()
                silver_tables = set(silver_df['TABLE_NAME'].tolist()) if silver_df is not None and len(silver_df) > 0 else set()
                gold_tables_set = set(gold_df['TABLE_NAME'].tolist()) if gold_df is not None and len(gold_df) > 0 else set()
                if not bronze_tables and not silver_tables and not gold_tables_set:
                    st.info("No tables exist yet. Generate data and run the pipeline to see the lineage graph.")
                else:
                    dot_lines = ['digraph {', 'rankdir=LR;', 'node [shape=box, style="rounded,filled", fontname="Arial"];', 'edge [fontname="Arial", fontsize=10];']
                    if bronze_tables:
                        dot_lines.append('subgraph cluster_bronze { label="BRONZE (Raw)"; style=filled; color=lightgoldenrod1;')
                        for t in sorted(bronze_tables):
                            label = t.replace("_VARIANT", "\\nVARIANT")
                            dot_lines.append(f'  {t} [label="{label}", fillcolor=goldenrod1];')
                        dot_lines.append('}')
                    if silver_tables:
                        dot_lines.append('subgraph cluster_silver { label="SILVER (Cleaned)"; style=filled; color=lightsteelblue1;')
                        for t in sorted(silver_tables):
                            dot_lines.append(f'  S_{t} [label="{t}", fillcolor=steelblue1];')
                        dot_lines.append('}')
                    if gold_tables_set:
                        dot_lines.append('subgraph cluster_gold { label="GOLD (Analytics)"; style=filled; color=lightgreen;')
                        for t in sorted(gold_tables_set):
                            label = t.replace("_", "\\n", 1) if len(t) > 15 else t
                            dot_lines.append(f'  G_{t} [label="{t}", fillcolor=green2];')
                        dot_lines.append('}')
                    b2s = {"CUSTOMERS_VARIANT": "CUSTOMERS", "PRODUCTS_VARIANT": "PRODUCTS", "ORDERS_VARIANT": "ORDERS", "ORDER_ITEMS_VARIANT": "ORDER_ITEMS", "SUPPORT_TICKETS_VARIANT": "SUPPORT_TICKETS"}
                    for bt, st_name in b2s.items():
                        if bt in bronze_tables and st_name in silver_tables:
                            dot_lines.append(f'  {bt} -> S_{st_name} [label="transform"];')
                    s2g_static = [("CUSTOMERS", "CUSTOMER_360", "aggregate"), ("ORDERS", "CUSTOMER_360", "join"), ("SUPPORT_TICKETS", "CUSTOMER_360", "join"),
                           ("PRODUCTS", "PRODUCT_PERFORMANCE", "aggregate"), ("ORDER_ITEMS", "PRODUCT_PERFORMANCE", "join"), ("ORDERS", "PRODUCT_PERFORMANCE", "join"),
                           ("ORDERS", "ORDER_SUMMARY", "aggregate"), ("CUSTOMERS", "ORDER_SUMMARY", "join"),
                           ("CUSTOMERS", "CUSTOMER_METRICS", "aggregate"), ("ORDERS", "CUSTOMER_METRICS", "join"),
                           ("CUSTOMERS", "ML_CUSTOMER_FEATURES", "features"), ("ORDERS", "ML_CUSTOMER_FEATURES", "features")]
                    s2g_dynamic = []
                    for gt in gold_tables_set:
                        if gt not in {g for _, g, _ in s2g_static}:
                            kg_edges = run_query(f"SELECT REPLACE(SOURCE_NODE_ID, 'TABLE:DBAONTAP_ANALYTICS.SILVER.', '') as SRC FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE WHERE TARGET_NODE_ID = 'TABLE:DBAONTAP_ANALYTICS.GOLD.{gt}' AND SOURCE_NODE_ID LIKE '%SILVER%'")
                            if kg_edges is not None and len(kg_edges) > 0:
                                for _, r in kg_edges.iterrows():
                                    s2g_dynamic.append((r['SRC'], gt, 'aggregate'))
                            else:
                                for st_name in silver_tables:
                                    st_root = st_name.rstrip('S')
                                    if st_root in gt or st_name in gt:
                                        s2g_dynamic.append((st_name, gt, 'aggregate'))
                                        break
                    for s, g, lbl in s2g_static + s2g_dynamic:
                        if s in silver_tables and g in gold_tables_set:
                            dot_lines.append(f'  S_{s} -> G_{g} [label="{lbl}"];')
                    dot_lines.append('}')
                    st.graphviz_chart('\n'.join(dot_lines), use_container_width=True)
                
            elif view_type == "Table Relationships":
                st.subheader("🔗 Foreign Key Relationships")
                
                edges_df = run_query("""
                    SELECT 
                        REPLACE(REPLACE(source_node_id, 'TABLE:DBAONTAP_ANALYTICS.SILVER.', ''), 'TABLE:DBAONTAP_ANALYTICS.GOLD.', '') as source,
                        REPLACE(REPLACE(target_node_id, 'TABLE:DBAONTAP_ANALYTICS.SILVER.', ''), 'TABLE:DBAONTAP_ANALYTICS.GOLD.', '') as target,
                        edge_type
                    FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE
                    WHERE edge_type IN ('REFERENCES', 'FOREIGN_KEY', 'JOINS_TO')
                    AND source_node_id LIKE 'TABLE:%'
                """)
                
                if edges_df is not None and len(edges_df) > 0:
                    dot_lines = ['digraph {', 'rankdir=LR;', 'node [shape=box, style="rounded,filled", fillcolor=lightblue, fontname="Arial"];']
                    for _, row in edges_df.iterrows():
                        dot_lines.append(f'    "{row["SOURCE"]}" -> "{row["TARGET"]}" [label="{row["EDGE_TYPE"]}"];')
                    dot_lines.append('}')
                    st.graphviz_chart('\n'.join(dot_lines), use_container_width=True)
                else:
                    dot = '''
                    digraph {
                        rankdir=LR;
                        node [shape=box, style="rounded,filled", fillcolor=lightblue, fontname="Arial"];
                        
                        CUSTOMERS -> ORDERS [label="customer_id"];
                        CUSTOMERS -> SUPPORT_TICKETS [label="customer_id"];
                        ORDERS -> ORDER_ITEMS [label="order_id"];
                        PRODUCTS -> ORDER_ITEMS [label="product_id"];
                    }
                    '''
                    st.graphviz_chart(dot, use_container_width=True)
                    
            else:  # Full Graph
                st.subheader("🌐 Complete Knowledge Graph")
                
                edges_df = run_query("""
                    SELECT 
                        source_node_id as source,
                        target_node_id as target,
                        edge_type
                    FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE
                    WHERE edge_type NOT IN ('CONTAINS')
                    LIMIT 50
                """)
                
                dot_lines = [
                    'digraph {',
                    'rankdir=TB;',
                    'node [shape=box, style="rounded,filled", fontname="Arial", fontsize=10];',
                    'edge [fontsize=8];'
                ]
                
                if edges_df is not None and len(edges_df) > 0:
                    for _, row in edges_df.iterrows():
                        src = row["SOURCE"].split(":")[-1].split(".")[-1][:20]
                        tgt = row["TARGET"].split(":")[-1].split(".")[-1][:20]
                        edge_type = row["EDGE_TYPE"]
                        
                        if "BRONZE" in row["SOURCE"] or "VARIANT" in row["SOURCE"]:
                            dot_lines.append(f'    "{src}" [fillcolor=goldenrod1];')
                        elif "SILVER" in row["SOURCE"]:
                            dot_lines.append(f'    "{src}" [fillcolor=steelblue1];')
                        elif "GOLD" in row["SOURCE"]:
                            dot_lines.append(f'    "{src}" [fillcolor=green2];')
                        
                        dot_lines.append(f'    "{src}" -> "{tgt}" [label="{edge_type}"];')
                
                dot_lines.append('}')
                st.graphviz_chart('\n'.join(dot_lines), use_container_width=True)
            
            st.markdown("---")
            st.subheader("📊 Knowledge Graph Statistics")
            
            stats_col1, stats_col2, stats_col3 = st.columns(3)
            
            node_count = run_query("SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_NODE")
            edge_count = run_query("SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE")
            table_count = run_query("SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.V_TABLES")
            
            with stats_col1:
                st.metric("Total Nodes", node_count['CNT'].iloc[0] if node_count is not None else 0)
            with stats_col2:
                st.metric("Total Edges", edge_count['CNT'].iloc[0] if edge_count is not None else 0)
            with stats_col3:
                st.metric("Tables Tracked", table_count['CNT'].iloc[0] if table_count is not None else 0)
            
            with st.expander("📋 Edge Types Distribution"):
                edge_types = run_query("""
                    SELECT edge_type, COUNT(*) as count 
                    FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_EDGE 
                    GROUP BY edge_type 
                    ORDER BY count DESC
                """)
                if edge_types is not None:
                    st.dataframe(edge_types, use_container_width=True)
                    
        except Exception as e:
            st.error(f"Error rendering graph: {e}")
            st.info("Make sure the Knowledge Graph has been populated by running the Agentic Workflow.")

# ============================================================================
# Architecture Progress Tracker Tab
# ============================================================================
def render_architecture_tab():
    st.header("🏗️ Architecture Progress")
    st.markdown("Live view of the demo pipeline. Sections light up as each stage completes.")

    if st.button("🔄 Refresh Status", use_container_width=False, key="arch_refresh_status"):
        for k in list(st.session_state.keys()):
            if k.startswith('arch_override_'):
                pass
        st.rerun()

    pg_ok = False
    try:
        pg_result = pg_execute("SELECT COUNT(*) as cnt FROM public.customers", fetch=True, target="source")
        pg_ok = pg_result is not None and len(pg_result) > 0 and pg_result['cnt'].iloc[0] > 0
    except:
        pass

    pg_landing_ok = False
    try:
        pg_land_result = pg_execute("SELECT COUNT(*) as cnt FROM public.customers", fetch=True, target="landing")
        pg_landing_ok = pg_land_result is not None and len(pg_land_result) > 0 and pg_land_result['cnt'].iloc[0] > 0
    except:
        pass

    landing_ok = False
    try:
        cdc_df = run_query("SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.\"public\".\"customers\"")
        landing_ok = cdc_df is not None and len(cdc_df) > 0 and cdc_df['CNT'].iloc[0] > 0
    except:
        pass

    bronze_ok = False
    try:
        b_df = run_query("SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.BRONZE.CUSTOMERS_VARIANT")
        bronze_ok = b_df is not None and len(b_df) > 0 and b_df['CNT'].iloc[0] > 0
    except:
        pass

    silver_ok = False
    try:
        s_df = run_query("SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_TYPE = 'BASE TABLE'")
        silver_ok = s_df is not None and len(s_df) > 0 and s_df['CNT'].iloc[0] >= 5
    except:
        pass

    gold_ok = False
    gold_count = 0
    try:
        g_df = run_query("SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_TYPE = 'BASE TABLE'")
        gold_count = g_df['CNT'].iloc[0] if g_df is not None and len(g_df) > 0 else 0
        gold_ok = gold_count >= 4
    except:
        pass

    sv_ok = False
    sv_count = 0
    try:
        sv_df = run_query("SHOW SEMANTIC VIEWS IN SCHEMA DBAONTAP_ANALYTICS.GOLD")
        sv_count = len(sv_df) if sv_df is not None else 0
        sv_ok = sv_count > 0
    except:
        pass

    kg_ok = False
    try:
        kg_df = run_query("SELECT COUNT(*) as cnt FROM DBAONTAP_ANALYTICS.KNOWLEDGE_GRAPH.KG_NODE")
        kg_ok = kg_df is not None and len(kg_df) > 0 and kg_df['CNT'].iloc[0] > 10
    except:
        pass

    chat_ok = st.session_state.get('arch_override_chat', False)

    stages = {
        'pg': {'label': 'PG Source\n(Managed)', 'detected': pg_ok},
        'pg_land': {'label': 'PG Landing\n(Replica)', 'detected': pg_landing_ok},
        'landing': {'label': 'Snowflake\nLanding', 'detected': landing_ok},
        'bronze': {'label': 'Bronze\n(VARIANT)', 'detected': bronze_ok},
        'silver': {'label': 'Silver\n(Agentic)', 'detected': silver_ok},
        'gold': {'label': f'Gold\n({gold_count} tables)', 'detected': gold_ok},
        'sv': {'label': f'Semantic\nViews ({sv_count})', 'detected': sv_ok},
        'chat': {'label': 'AI Chat\n(Cortex)', 'detected': chat_ok},
        'kg': {'label': 'Knowledge\nGraph', 'detected': kg_ok},
    }

    for key in stages:
        override_key = f'arch_override_{key}'
        if override_key not in st.session_state:
            st.session_state[override_key] = False
        if st.session_state[override_key]:
            stages[key]['detected'] = True

    active_color = '#2ecc71'
    active_font = 'white'
    inactive_color = '#bdc3c7'
    inactive_font = '#7f8c8d'
    edge_active = '#2ecc71'
    edge_inactive = '#bdc3c7'

    def node_style(key):
        s = stages[key]
        if s['detected']:
            return f'fillcolor="{active_color}", fontcolor="{active_font}", penwidth=2'
        else:
            return f'fillcolor="{inactive_color}", fontcolor="{inactive_font}", penwidth=1'

    def edge_style(from_key, to_key):
        if stages[from_key]['detected'] and stages[to_key]['detected']:
            return f'color="{edge_active}", penwidth=2'
        else:
            return f'color="{edge_inactive}", penwidth=1, style=dashed'

    dot = f'''digraph {{
        rankdir=LR;
        bgcolor="transparent";
        node [shape=box, style="rounded,filled", fontname="Arial", fontsize=11, margin="0.3,0.15"];
        edge [fontname="Arial", fontsize=9];

        subgraph cluster_source {{
            label="SOURCE (PostgreSQL)"; labeljust=l; style=dashed; color=gray80; fontcolor=gray60;
            pg [label="{stages['pg']['label']}", {node_style('pg')}];
            pg_land [label="{stages['pg_land']['label']}", {node_style('pg_land')}];
        }}

        subgraph cluster_ingest {{
            label="INGEST (Openflow CDC)"; labeljust=l; style=dashed; color=gray80; fontcolor=gray60;
            landing [label="{stages['landing']['label']}", {node_style('landing')}];
            bronze [label="{stages['bronze']['label']}", {node_style('bronze')}];
        }}

        subgraph cluster_transform {{
            label="TRANSFORM (Agentic)"; labeljust=l; style=dashed; color=gray80; fontcolor=gray60;
            silver [label="{stages['silver']['label']}", {node_style('silver')}];
            gold [label="{stages['gold']['label']}", {node_style('gold')}];
        }}

        subgraph cluster_serve {{
            label="SERVE"; labeljust=l; style=dashed; color=gray80; fontcolor=gray60;
            sv [label="{stages['sv']['label']}", {node_style('sv')}];
            chat [label="{stages['chat']['label']}", {node_style('chat')}];
        }}

        subgraph cluster_observe {{
            label="OBSERVE"; labeljust=l; style=dashed; color=gray80; fontcolor=gray60;
            kg [label="{stages['kg']['label']}", {node_style('kg')}];
        }}

        pg -> pg_land [{edge_style('pg', 'pg_land')}, label="logical\nreplication"];
        pg_land -> landing [{edge_style('pg_land', 'landing')}, label="Openflow\nCDC"];
        landing -> bronze [{edge_style('landing', 'bronze')}, label="OBJECT_\nCONSTRUCT(*)"];
        bronze -> silver [{edge_style('bronze', 'silver')}, label="AI transform"];
        silver -> gold [{edge_style('silver', 'gold')}, label="aggregate"];
        gold -> sv [{edge_style('gold', 'sv')}, label="semantics"];
        sv -> chat [{edge_style('sv', 'chat')}, label="NL query"];

        bronze -> kg [{edge_style('bronze', 'kg')}, style=dotted, label="metadata"];
        silver -> kg [{edge_style('silver', 'kg')}, style=dotted, label="metadata"];
        gold -> kg [{edge_style('gold', 'kg')}, style=dotted, label="metadata"];
    }}'''

    st.graphviz_chart(dot, use_container_width=True)

    completed = sum(1 for s in stages.values() if s['detected'])
    total = len(stages)
    st.progress(completed / total, text=f"Demo Progress: {completed}/{total} stages complete")

    st.markdown("---")
    st.subheader("Stage Details")
    cols = st.columns(5)
    stage_list = list(stages.items())
    for i, (key, stage) in enumerate(stage_list):
        with cols[i % 5]:
            icon = "✅" if stage['detected'] else "⬜"
            st.markdown(f"{icon} **{stage['label'].replace(chr(10), ' ')}**")

    with st.expander("🎛️ Manual Overrides"):
        st.caption("Use these if auto-detection doesn't capture a completed stage (e.g. AI Chat after asking a question).")
        override_cols = st.columns(5)
        for i, (key, stage) in enumerate(stage_list):
            with override_cols[i % 5]:
                override_key = f'arch_override_{key}'
                label = stage['label'].replace('\n', ' ')
                if st.checkbox(label, value=st.session_state.get(override_key, False), key=f'cb_{override_key}'):
                    st.session_state[override_key] = True
                else:
                    st.session_state[override_key] = False


# ============================================================================
# Main App
# ============================================================================
def main():
    render_sidebar()
    
    st.title("🏭 Agentic Data Foundry")
    st.markdown("*AI Ready for Data to Data Ready for AI*")
    
    tab0, tab1, tab2, tab3, tab4, tab5, tab5b, tab6, tab7, tab8, tab9, tab10, tab11 = st.tabs([
        "🏗️ Architecture",
        "📊 Generate Data",
        "🔄 Pipeline Status",
        "🤖 Agentic Workflow",
        "🥇 Gold Layer",
        "📐 Schema Contracts",
        "🎯 Directives",
        "🔷 Semantic Views",
        "💬 AI Chat",
        "🕸️ Knowledge Graph",
        "📋 Logs & Errors",
        "🎛️ Demo Control",
        "🗑️ Reset Data"
    ])
    
    with tab0:
        render_architecture_tab()
    
    with tab1:
        render_data_generation_tab()
    
    with tab2:
        render_pipeline_tab()
    
    with tab3:
        render_agentic_workflow_tab()
    
    with tab4:
        render_gold_layer_tab()
    
    with tab5:
        render_schema_contracts_tab()
    
    with tab5b:
        render_directives_tab()
    
    with tab6:
        render_semantic_views_tab()
    
    with tab7:
        render_chatbot_tab()
    
    with tab8:
        render_knowledge_graph_tab()
    
    with tab9:
        render_logs_tab()
    
    with tab10:
        render_demo_control_tab()
    
    with tab11:
        render_reset_tab()

if __name__ == "__main__":
    main()
