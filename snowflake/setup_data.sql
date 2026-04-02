-- CREATE TABLES AND ADD DATA
USE ROLE SYSADMIN;
USE SCHEMA RAW_DEV.RAW;

--- 1. CUSTOMERS ---
CREATE OR REPLACE TABLE CUSTOMERS AS
SELECT 
    column1 as cust_number,
    column2 as fullname,
    column3 as email_address,
    column4 as city,
    column5 as country,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP()))) as updated_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'DEV' as src
FROM VALUES
    ('CUST_001', 'Jean Dupont', 'jean.dupont@email.com', 'Paris', 'France', -5),
    ('CUST_002', 'Marie Dubois', 'marie.dubois@email.com', 'Marseille', 'France', -4),
    ('CUST_003', 'Pierre Martin', 'pierre.martin@email.com', 'Lyon', 'France', -3),
    ('CUST_004', 'Sophie Bernard', 'sophie.bernard@email.com', 'Toulouse', 'France', -2),
    ('CUST_005', 'Lucie Petit', 'lucie.petit@email.com', 'Nice', 'France', -1);

--- 2. PRODUCTS ---
CREATE OR REPLACE TABLE PRODUCTS AS
SELECT 
    column1 as sku,
    column2 as item_desc,
    column3 as category,
    column4 as material_type,
    column5 as unit_price,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP()))) as updated_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'DEV' as src
FROM VALUES
    ('PROD_001', 'Sac à main bandoulière', 'Sacs', 'Cuir', 350.00, -1),
    ('PROD_002', 'Portefeuille compagnon', 'Portefeuilles', 'Cuir', 120.00, -3),
    ('PROD_003', 'Ceinture réversible', 'Ceintures', 'Cuir', 85.50, -2),
    ('PROD_004', 'Sac à dos urbain', 'Sacs', 'Toile', 480.00, -4),
    ('PROD_005', 'Porte-cartes minimaliste', 'Portefeuilles', 'Toile', 60.00, -5);

--- 3. ORDERS ---
CREATE OR REPLACE TABLE ORDERS AS
SELECT 
    column1 as order_ref,
    column2 as cust_number,
    column3 as sku,
    column4 as qty,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column5, CURRENT_TIMESTAMP()))) as ordered_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'DEV' as src
FROM VALUES
    ('ORD_1001', 'CUST_001', 'PROD_001', 1, -2880),
    ('ORD_1002', 'CUST_002', 'PROD_002', 2, -1440),
    ('ORD_1003', 'CUST_003', 'PROD_003', 1, -720),
    ('ORD_1004', 'CUST_001', 'PROD_005', 3, -120),
    ('ORD_1005', 'CUST_004', 'PROD_004', 1, -30);

-- CREATE TABLES AND ADD DATA
USE ROLE SYSADMIN;
USE SCHEMA RAW_PRD.RAW;

--- 1. CUSTOMERS ---
CREATE OR REPLACE TABLE CUSTOMERS AS
SELECT 
    column1 as cust_number,
    column2 as fullname,
    column3 as email_address,
    column4 as city,
    column5 as country,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP()))) as updated_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'PRD' as src
FROM VALUES
    ('CUST_101', 'Alice Morel', 'alice.morel@email.com', 'Bordeaux', 'France', -10),
    ('CUST_102', 'Thomas Roux', 'thomas.roux@email.com', 'Nantes', 'France', -8),
    ('CUST_103', 'Emma Lefebvre', 'emma.lefebvre@email.com', 'Lille', 'France', -6),
    ('CUST_104', 'Nicolas Petit', 'nicolas.petit@email.com', 'Strasbourg', 'France', -4),
    ('CUST_105', 'Chloe Garcia', 'chloe.garcia@email.com', 'Montpellier', 'France', -2);

--- 2. PRODUCTS ---
CREATE OR REPLACE TABLE PRODUCTS AS
SELECT 
    column1 as sku,
    column2 as item_desc,
    column3 as category,
    column4 as material_type,
    column5 as unit_price,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP()))) as updated_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'PRD' as src
FROM VALUES
    ('PROD_001', 'Sac à main bandoulière', 'Sacs', 'Cuir', 350.00, -10),
    ('PROD_002', 'Portefeuille compagnon', 'Portefeuilles', 'Cuir', 120.00, -20),
    ('PROD_003', 'Ceinture réversible', 'Ceintures', 'Cuir', 85.50, -15),
    ('PROD_004', 'Sac à dos urbain', 'Sacs', 'Toile', 480.00, -30),
    ('PROD_005', 'Porte-cartes minimaliste', 'Portefeuilles', 'Toile', 60.00, -45);

--- 3. ORDERS ---
CREATE OR REPLACE TABLE ORDERS AS
SELECT 
    column1 as order_ref,
    column2 as cust_number,
    column3 as sku,
    column4 as qty,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column5, CURRENT_TIMESTAMP()))) as ordered_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'PRD' as src
FROM VALUES
    ('ORD_2001', 'CUST_101', 'PROD_001', 1, -5760),
    ('ORD_2002', 'CUST_102', 'PROD_002', 1, -4320),
    ('ORD_2003', 'CUST_103', 'PROD_003', 2, -2880),
    ('ORD_2004', 'CUST_101', 'PROD_004', 1, -1440),
    ('ORD_2005', 'CUST_105', 'PROD_005', 5, -60);

----------------
-- PARAMETERIZED PROCEDURE FOR TRANSACTION GENERATION
USE WAREHOUSE ORCHESTRATION_WH;
CREATE OR REPLACE PROCEDURE ORCHESTRATION.JOBS.GENERATE_X_ORDERS(
    env VARCHAR,
    transaction_count NUMBER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = (
    'snowflake-snowpark-python',
    'pytz'
)
HANDLER = 'generate_and_insert_transactions'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, max as sf_max, substring
from snowflake.snowpark.types import IntegerType
import datetime
import random
import pytz 

def generate_and_insert_transactions(session: Session, env: str, transaction_count: int):
    env = env.upper()
    if env not in ('DEV', 'PRD'):
        return f"Error: Invalid environment '{env}'. Must be 'DEV' or 'PRD'."
    
    num_transactions = transaction_count
    target_table = f"RAW_{env}.RAW.ORDERS"
    customers_table = f"RAW_{env}.RAW.CUSTOMERS"
    products_table = f"RAW_{env}.RAW.PRODUCTS"

    customer_ids = session.table(customers_table).select(col("cust_number")).collect()
    customer_ids = [row[0] for row in customer_ids]
    if not customer_ids:
        return f"Error: No customers found in {customers_table}."

    product_ids = session.table(products_table).select(col("sku")).collect()
    product_ids = [row[0] for row in product_ids]
    if not product_ids:
        return f"Error: No products found in {products_table}."

    max_id_result = session.table(target_table).select(
        sf_max(substring(col("order_ref"), 5, 10).cast(IntegerType()))
    ).collect()
    max_id_num = max_id_result[0][0] if max_id_result[0][0] is not None else 0

    transactions_data = []
    paris_tz = pytz.timezone("Europe/Paris")
    today_date = datetime.datetime.now(paris_tz).date()
    now_paris = datetime.datetime.now(paris_tz)
    start_time_today_paris = paris_tz.localize(datetime.datetime(today_date.year, today_date.month, today_date.day, 8, 0, 0))
    end_time_today_paris = now_paris
    start_time_utc = start_time_today_paris.astimezone(pytz.utc)
    end_time_utc = end_time_today_paris.astimezone(pytz.utc)
    time_diff_for_random = end_time_utc - start_time_utc

    for i in range(num_transactions):
        order_ref = f"ORD_{max_id_num + i + 1:04d}"
        cust_number = random.choice(customer_ids)
        sku = random.choice(product_ids)
        qty = random.randint(1, 5)
        loaded_at = datetime.datetime.now(paris_tz).replace(tzinfo=None)
        random_seconds = random.uniform(0, time_diff_for_random.total_seconds())
        order_datetime_utc_aware = start_time_utc + datetime.timedelta(seconds=random_seconds)
        ordered_at = order_datetime_utc_aware.astimezone(paris_tz).replace(tzinfo=None)

        transactions_data.append((
            order_ref,
            cust_number,
            sku,
            qty,
            ordered_at,
            loaded_at,
            env
        ))

    df = session.create_dataframe(transactions_data, schema=[
        "order_ref", "cust_number", "sku", "qty", "ordered_at", "_loaded_at", "src"
    ])
    df.write.mode("append").save_as_table(target_table)

    return f"Successfully generated {num_transactions} transactions into {target_table} ({start_time_today_paris.strftime('%Y-%m-%d %H:%M:%S')} to {end_time_today_paris.strftime('%Y-%m-%d %H:%M:%S')} Paris time)."
$$;

-- TASKS FOR DEV AND PRD
USE ROLE SYSADMIN;
CREATE OR REPLACE TASK ORCHESTRATION.JOBS.TRANSACTION_GENERATION_DEV_TASK
  WAREHOUSE = ORCHESTRATION_WH
  SCHEDULE = 'USING CRON 0 22 * * * Europe/Paris'
  COMMENT = 'Generate new DEV transactions.'
AS
  CALL ORCHESTRATION.JOBS.GENERATE_X_ORDERS('DEV', 2);

CREATE OR REPLACE TASK ORCHESTRATION.JOBS.TRANSACTION_GENERATION_PRD_TASK
  WAREHOUSE = ORCHESTRATION_WH
  SCHEDULE = 'USING CRON 0 22 * * * Europe/Paris'
  COMMENT = 'Generate new PRD transactions.'
AS
  CALL ORCHESTRATION.JOBS.GENERATE_X_ORDERS('PRD', 2);

-- Execute manually for testing
CALL ORCHESTRATION.JOBS.GENERATE_X_ORDERS('DEV', 3);
SELECT * FROM RAW_DEV.RAW.ORDERS;

CALL ORCHESTRATION.JOBS.GENERATE_X_ORDERS('PRD', 3);
SELECT * FROM RAW_PRD.RAW.ORDERS;

-- Start/Suspend the tasks
USE ROLE ACCOUNTADMIN;
ALTER TASK ORCHESTRATION.JOBS.TRANSACTION_GENERATION_DEV_TASK RESUME; -- SUSPEND;
ALTER TASK ORCHESTRATION.JOBS.TRANSACTION_GENERATION_PRD_TASK RESUME; -- SUSPEND;
