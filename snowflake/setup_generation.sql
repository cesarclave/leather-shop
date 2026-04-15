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

-- PARAMETERIZED PROCEDURE FOR CUSTOMER GENERATION
CREATE OR REPLACE PROCEDURE ORCHESTRATION.JOBS.GENERATE_X_CUSTOMERS(
    env VARCHAR,
    customer_count NUMBER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = (
    'snowflake-snowpark-python',
    'pytz',
    'faker'
)
HANDLER = 'generate_and_insert_customers'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, max as sf_max, substring
from snowflake.snowpark.types import IntegerType
import datetime
import random
import pytz
from faker import Faker

def generate_and_insert_customers(session: Session, env: str, customer_count: int):
    env = env.upper()
    if env not in ('DEV', 'PRD'):
        return f"Error: Invalid environment '{env}'. Must be 'DEV' or 'PRD'."

    target_table = f"RAW_{env}.RAW.CUSTOMERS"
    fake = Faker('fr_FR')

    max_id_result = session.table(target_table).select(
        sf_max(substring(col("cust_number"), 6, 10).cast(IntegerType()))
    ).collect()
    max_id_num = max_id_result[0][0] if max_id_result[0][0] is not None else 0

    french_cities = [
        'Paris', 'Marseille', 'Lyon', 'Toulouse', 'Nice', 'Nantes',
        'Strasbourg', 'Montpellier', 'Bordeaux', 'Lille', 'Rennes',
        'Reims', 'Toulon', 'Le Havre', 'Grenoble', 'Dijon', 'Angers',
        'Saint-Etienne', 'Brest', 'Aix-en-Provence'
    ]

    customers_data = []
    paris_tz = pytz.timezone("Europe/Paris")
    now_paris = datetime.datetime.now(paris_tz)

    for i in range(customer_count):
        cust_number = f"CUST_{max_id_num + i + 1:03d}"
        first_name = fake.first_name()
        last_name = fake.last_name()
        fullname = f"{first_name} {last_name}"
        email_address = f"{first_name.lower()}.{last_name.lower()}@email.com"
        city = random.choice(french_cities)
        country = 'France'
        random_offset_minutes = random.randint(-60, 0)
        updated_at = (now_paris + datetime.timedelta(minutes=random_offset_minutes)).replace(tzinfo=None)
        loaded_at = now_paris.replace(tzinfo=None)

        customers_data.append((
            cust_number,
            fullname,
            email_address,
            city,
            country,
            updated_at,
            loaded_at,
            env
        ))

    df = session.create_dataframe(customers_data, schema=[
        "cust_number", "fullname", "email_address", "city", "country",
        "updated_at", "_loaded_at", "src"
    ])
    df.write.mode("append").save_as_table(target_table)

    return f"Successfully generated {customer_count} customers into {target_table}."
$$;

----------------
USE WAREHOUSE ORCHESTRATION_WH;
-- PARAMETERIZED PROCEDURE FOR PRODUCT GENERATION
CREATE OR REPLACE PROCEDURE ORCHESTRATION.JOBS.GENERATE_X_PRODUCTS(
    env VARCHAR,
    product_count NUMBER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = (
    'snowflake-snowpark-python',
    'pytz',
    'faker'
)
HANDLER = 'generate_and_insert_products'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, max as sf_max, substring
from snowflake.snowpark.types import IntegerType
import datetime
import random
import pytz
from faker import Faker

def generate_and_insert_products(session: Session, env: str, product_count: int):
    env = env.upper()
    if env not in ('DEV', 'PRD'):
        return f"Error: Invalid environment '{env}'. Must be 'DEV' or 'PRD'."

    target_table = f"RAW_{env}.RAW.PRODUCTS"
    fake = Faker('fr_FR')

    max_id_result = session.table(target_table).select(
        sf_max(substring(col("sku"), 6, 10).cast(IntegerType()))
    ).collect()
    max_id_num = max_id_result[0][0] if max_id_result[0][0] is not None else 0

    categories_items = {
        'Sacs': [
            'Sac à main classique', 'Sac bandoulière élégant', 'Sac de voyage compact',
            'Sac à dos urbain', 'Sac cabas raffiné', 'Sac pochette soirée',
            'Sac seau tendance', 'Sac besace vintage'
        ],
        'Portefeuilles': [
            'Portefeuille long zippé', 'Portefeuille compagnon', 'Porte-cartes minimaliste',
            'Porte-monnaie à fermoir', 'Portefeuille bifold classique',
            'Porte-cartes accordéon', 'Portefeuille voyage'
        ],
        'Ceintures': [
            'Ceinture réversible', 'Ceinture tressée artisanale', 'Ceinture fine dorée',
            'Ceinture large à boucle', 'Ceinture classique homme',
            'Ceinture élastique sport', 'Ceinture chaîne décorative'
        ]
    }
    materials = ['Cuir', 'Toile']
    price_ranges = {
        'Sacs': (180.00, 550.00),
        'Portefeuilles': (50.00, 200.00),
        'Ceintures': (60.00, 150.00)
    }

    products_data = []
    paris_tz = pytz.timezone("Europe/Paris")
    now_paris = datetime.datetime.now(paris_tz)

    for i in range(product_count):
        sku = f"PROD_{max_id_num + i + 1:03d}"
        category = random.choice(list(categories_items.keys()))
        item_desc = random.choice(categories_items[category])
        material_type = random.choice(materials)
        min_price, max_price = price_ranges[category]
        unit_price = round(random.uniform(min_price, max_price), 2)
        random_offset_minutes = random.randint(-60, 0)
        updated_at = (now_paris + datetime.timedelta(minutes=random_offset_minutes)).replace(tzinfo=None)
        loaded_at = now_paris.replace(tzinfo=None)

        products_data.append((
            sku,
            item_desc,
            category,
            material_type,
            unit_price,
            updated_at,
            loaded_at,
            env
        ))

    df = session.create_dataframe(products_data, schema=[
        "sku", "item_desc", "category", "material_type", "unit_price",
        "updated_at", "_loaded_at", "src"
    ])
    df.write.mode("append").save_as_table(target_table)

    return f"Successfully generated {product_count} products into {target_table}."
$$;

-- TASKS FOR DEV AND PRD
USE ROLE SYSADMIN;

--- ORDERS: every hour
CREATE OR REPLACE TASK ORCHESTRATION.JOBS.TRANSACTION_GENERATION_DEV_TASK
  WAREHOUSE = ORCHESTRATION_WH
  SCHEDULE = 'USING CRON 0 * * * * Europe/Paris'
  COMMENT = 'Generate new DEV transactions every hour.'
AS
  CALL ORCHESTRATION.JOBS.GENERATE_X_ORDERS('DEV', 2);

CREATE OR REPLACE TASK ORCHESTRATION.JOBS.TRANSACTION_GENERATION_PRD_TASK
  WAREHOUSE = ORCHESTRATION_WH
  SCHEDULE = 'USING CRON 0 * * * * Europe/Paris'
  COMMENT = 'Generate new PRD transactions every hour.'
AS
  CALL ORCHESTRATION.JOBS.GENERATE_X_ORDERS('PRD', 2);

--- CUSTOMERS: every 3 hours
CREATE OR REPLACE TASK ORCHESTRATION.JOBS.CUSTOMER_GENERATION_DEV_TASK
  WAREHOUSE = ORCHESTRATION_WH
  SCHEDULE = 'USING CRON 0 */3 * * * Europe/Paris'
  COMMENT = 'Generate new DEV customers every 3 hours.'
AS
  CALL ORCHESTRATION.JOBS.GENERATE_X_CUSTOMERS('DEV', 2);

CREATE OR REPLACE TASK ORCHESTRATION.JOBS.CUSTOMER_GENERATION_PRD_TASK
  WAREHOUSE = ORCHESTRATION_WH
  SCHEDULE = 'USING CRON 0 */3 * * * Europe/Paris'
  COMMENT = 'Generate new PRD customers every 3 hours.'
AS
  CALL ORCHESTRATION.JOBS.GENERATE_X_CUSTOMERS('PRD', 2);

--- PRODUCTS: every 6 hours
CREATE OR REPLACE TASK ORCHESTRATION.JOBS.PRODUCT_GENERATION_DEV_TASK
  WAREHOUSE = ORCHESTRATION_WH
  SCHEDULE = 'USING CRON 0 */6 * * * Europe/Paris'
  COMMENT = 'Generate new DEV products every 6 hours.'
AS
  CALL ORCHESTRATION.JOBS.GENERATE_X_PRODUCTS('DEV', 2);

CREATE OR REPLACE TASK ORCHESTRATION.JOBS.PRODUCT_GENERATION_PRD_TASK
  WAREHOUSE = ORCHESTRATION_WH
  SCHEDULE = 'USING CRON 0 */6 * * * Europe/Paris'
  COMMENT = 'Generate new PRD products every 6 hours.'
AS
  CALL ORCHESTRATION.JOBS.GENERATE_X_PRODUCTS('PRD', 2);

-- Execute manually for testing
CALL ORCHESTRATION.JOBS.GENERATE_X_ORDERS('DEV', 3);
SELECT * FROM RAW_DEV.RAW.ORDERS;

CALL ORCHESTRATION.JOBS.GENERATE_X_ORDERS('PRD', 3);
SELECT * FROM RAW_PRD.RAW.ORDERS;

-- Execute manually for testing (Customers)
SELECT * FROM RAW_DEV.RAW.CUSTOMERS;
CALL ORCHESTRATION.JOBS.GENERATE_X_CUSTOMERS('DEV', 2);
SELECT * FROM RAW_DEV.RAW.CUSTOMERS;

CALL ORCHESTRATION.JOBS.GENERATE_X_CUSTOMERS('PRD', 3);
SELECT * FROM RAW_PRD.RAW.CUSTOMERS;

-- Execute manually for testing (Products)
CALL ORCHESTRATION.JOBS.GENERATE_X_PRODUCTS('DEV', 3);
SELECT * FROM RAW_DEV.RAW.PRODUCTS;

CALL ORCHESTRATION.JOBS.GENERATE_X_PRODUCTS('PRD', 3);
SELECT * FROM RAW_PRD.RAW.PRODUCTS;

-- Start/Suspend the tasks
USE ROLE ACCOUNTADMIN;
ALTER TASK ORCHESTRATION.JOBS.TRANSACTION_GENERATION_DEV_TASK RESUME; -- SUSPEND;
ALTER TASK ORCHESTRATION.JOBS.CUSTOMER_GENERATION_DEV_TASK RESUME; -- SUSPEND;
ALTER TASK ORCHESTRATION.JOBS.PRODUCT_GENERATION_DEV_TASK RESUME; -- SUSPEND;

ALTER TASK ORCHESTRATION.JOBS.TRANSACTION_GENERATION_PRD_TASK SUSPEND; -- RESUME;
ALTER TASK ORCHESTRATION.JOBS.CUSTOMER_GENERATION_PRD_TASK SUSPEND; -- RESUME;
ALTER TASK ORCHESTRATION.JOBS.PRODUCT_GENERATION_PRD_TASK SUSPEND; -- RESUME;
