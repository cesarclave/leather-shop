---------------------------------------------------------------------------------------------------------------------------
USE ROLE SYSADMIN;
USE SCHEMA RAW_DEV.RAW;

--- 1. CUSTOMERS ---
CREATE OR REPLACE TABLE CUSTOMERS AS
SELECT 
    column1::VARCHAR(36) as CUSTOMER_ID,
    column2::VARCHAR(100) as CUSTOMER_NAME,
    column3::VARCHAR(100) as EMAIL,
    column4::VARCHAR(50) as CITY,
    column5::VARCHAR(50) as COUNTRY,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP())))::TIMESTAMP_NTZ as LAST_UPDATE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as LOAD_TIMESTAMP,
    'DEV'::VARCHAR(10) as source_env
FROM VALUES
    ('CUST_001', 'Jean Dupont', 'jean.dupont@email.com', 'Paris', 'France', -5),
    ('CUST_002', 'Marie Dubois', 'marie.dubois@email.com', 'Marseille', 'France', -4),
    ('CUST_003', 'Pierre Martin', 'pierre.martin@email.com', 'Lyon', 'France', -3),
    ('CUST_004', 'Sophie Bernard', 'sophie.bernard@email.com', 'Toulouse', 'France', -2),
    ('CUST_005', 'Lucie Petit', 'lucie.petit@email.com', 'Nice', 'France', -1);
select * from CUSTOMERS;
--- 2. PRODUCTS ---
CREATE OR REPLACE TABLE PRODUCTS AS
SELECT 
    column1::VARCHAR(36) as PRODUCT_ID,
    column2::VARCHAR(100) as PRODUCT_NAME,
    column3::VARCHAR(50) as CATEGORY,
    column4::VARCHAR(50) as MATERIAL,
    column5::NUMBER(10, 2) as PRICE,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP())))::TIMESTAMP_NTZ as LAST_UPDATE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as LOAD_TIMESTAMP,
    'DEV'::VARCHAR(10) as source_env
FROM VALUES
    ('PROD_001', 'Sac à main bandoulière', 'Sacs', 'Cuir', 350.00, -1),
    ('PROD_002', 'Portefeuille compagnon', 'Portefeuilles', 'Cuir', 120.00, -3),
    ('PROD_003', 'Ceinture réversible', 'Ceintures', 'Cuir', 85.50, -2),
    ('PROD_004', 'Sac à dos urbain', 'Sacs', 'Toile', 480.00, -4),
    ('PROD_005', 'Porte-cartes minimaliste', 'Portefeuilles', 'Toile', 60.00, -5);
select * from PRODUCTS;
--- 3. TRANSACTIONS ---
CREATE OR REPLACE TABLE TRANSACTIONS AS
SELECT 
    column1::VARCHAR(36) as TRANSACTION_ID,
    column2::VARCHAR(36) as CUSTOMER_ID,
    column3::VARCHAR(36) as PRODUCT_ID,
    column4::NUMBER(10) as QUANTITY,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column5, CURRENT_TIMESTAMP())))::TIMESTAMP_NTZ as TRANSACTION_DATETIME,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as LOAD_TIMESTAMP,
    'DEV'::VARCHAR(10) as source_env
FROM VALUES
    ('TXN_1001', 'CUST_001', 'PROD_001', 1, -2880), -- -2 days
    ('TXN_1002', 'CUST_002', 'PROD_002', 2, -1440), -- -1 day
    ('TXN_1003', 'CUST_003', 'PROD_003', 1, -720),  -- -12 hours
    ('TXN_1004', 'CUST_001', 'PROD_005', 3, -120),  -- -2 hours
    ('TXN_1005', 'CUST_004', 'PROD_004', 1, -30);   -- -30 mins  
select * from TRANSACTIONS;

---------------------------------------------------------------------------------------------------------------------------
USE ROLE SYSADMIN;
USE SCHEMA RAW_PRD.RAW;

--- 1. CUSTOMERS ---
CREATE OR REPLACE TABLE CUSTOMERS AS
SELECT 
    column1::VARCHAR(36) as CUSTOMER_ID,
    column2::VARCHAR(100) as CUSTOMER_NAME,
    column3::VARCHAR(100) as EMAIL,
    column4::VARCHAR(50) as CITY,
    column5::VARCHAR(50) as COUNTRY,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP())))::TIMESTAMP_NTZ as LAST_UPDATE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as LOAD_TIMESTAMP,
    'PRD'::VARCHAR(3) as SOURCE_ENV
FROM VALUES
    ('CUST_101', 'Alice Morel', 'alice.morel@email.com', 'Bordeaux', 'France', -10),
    ('CUST_102', 'Thomas Roux', 'thomas.roux@email.com', 'Nantes', 'France', -8),
    ('CUST_103', 'Emma Lefebvre', 'emma.lefebvre@email.com', 'Lille', 'France', -6),
    ('CUST_104', 'Nicolas Petit', 'nicolas.petit@email.com', 'Strasbourg', 'France', -4),
    ('CUST_105', 'Chloe Garcia', 'chloe.garcia@email.com', 'Montpellier', 'France', -2);

--- 2. PRODUCTS ---
CREATE OR REPLACE TABLE PRODUCTS AS
SELECT 
    column1::VARCHAR(36) as PRODUCT_ID,
    column2::VARCHAR(100) as PRODUCT_NAME,
    column3::VARCHAR(50) as CATEGORY,
    column4::VARCHAR(50) as MATERIAL,
    column5::NUMBER(10, 2) as PRICE,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP())))::TIMESTAMP_NTZ as LAST_UPDATE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as LOAD_TIMESTAMP,
    'PRD'::VARCHAR(3) as SOURCE_ENV
FROM VALUES
    ('PROD_001', 'Sac à main bandoulière', 'Sacs', 'Cuir', 350.00, -10),
    ('PROD_002', 'Portefeuille compagnon', 'Portefeuilles', 'Cuir', 120.00, -20),
    ('PROD_003', 'Ceinture réversible', 'Ceintures', 'Cuir', 85.50, -15),
    ('PROD_004', 'Sac à dos urbain', 'Sacs', 'Toile', 480.00, -30),
    ('PROD_005', 'Porte-cartes minimaliste', 'Portefeuilles', 'Toile', 60.00, -45);

--- 3. TRANSACTIONS ---
CREATE OR REPLACE TABLE TRANSACTIONS AS
SELECT 
    column1::VARCHAR(36) as TRANSACTION_ID,
    column2::VARCHAR(36) as CUSTOMER_ID,
    column3::VARCHAR(36) as PRODUCT_ID,
    column4::NUMBER(10) as QUANTITY,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column5, CURRENT_TIMESTAMP())))::TIMESTAMP_NTZ as TRANSACTION_DATETIME,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ as LOAD_TIMESTAMP,
    'PRD'::VARCHAR(3) as SOURCE_ENV
FROM VALUES
    ('TXN_2001', 'CUST_101', 'PROD_001', 1, -5760), -- -4 days
    ('TXN_2002', 'CUST_102', 'PROD_002', 1, -4320), -- -3 days
    ('TXN_2003', 'CUST_103', 'PROD_003', 2, -2880), -- -2 days
    ('TXN_2004', 'CUST_101', 'PROD_004', 1, -1440), -- -1 day
    ('TXN_2005', 'CUST_105', 'PROD_005', 5, -60);   -- -1 hour


----------------
-- ADD dew data based on existing ones
USE WAREHOUSE _ORCHESTRATION_DEV_WH;
CREATE OR REPLACE PROCEDURE RAW_DEV._ORCHESTRATION.GENERATE_X_TRANSACTIONS(target_table_name VARCHAR, transaction_count NUMBER)
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
from snowflake.snowpark.functions import lit, col, max as sf_max, substring
from snowflake.snowpark.types import IntegerType
import datetime
import random
import pytz 

def generate_and_insert_transactions(session: Session, target_table_name: str, transaction_count: int):
    num_transactions = transaction_count 

    # --- Fetch existing customer and product IDs for referential integrity ---
    customer_ids = session.table("RAW_DEV.RAW.CUSTOMERS").select(col("CUSTOMER_ID")).collect()
    customer_ids = [row[0] for row in customer_ids]
    if not customer_ids:
        return "Error: No customers found in RAW_DEV.RAW.CUSTOMERS. Cannot generate transactions."

    product_ids = session.table("RAW_DEV.RAW.PRODUCTS").select(col("PRODUCT_ID")).collect()
    product_ids = [row[0] for row in product_ids]
    if not product_ids:
        return "Error: No products found in RAW_DEV.RAW.PRODUCTS. Cannot generate transactions."
    # --- End Fetch ---

    # --- Get max transaction ID for incremental pattern ---
    max_id_result = session.table(target_table_name).select(
        sf_max(substring(col("TRANSACTION_ID"), 5, 10).cast(IntegerType()))
    ).collect()
    max_id_num = max_id_result[0][0] if max_id_result[0][0] is not None else 0

    transactions_data = []

    # Define the Paris timezone
    paris_tz = pytz.timezone("Europe/Paris")

    # Get today's date and time in Paris time
    today_date = datetime.datetime.now(paris_tz).date()
    now_paris = datetime.datetime.now(paris_tz)
    
    # Define the start and end times for transactions on today's date, localized to Paris
    start_time_today_paris = paris_tz.localize(datetime.datetime(today_date.year, today_date.month, today_date.day, 8, 0, 0))
    end_time_today_paris = now_paris

    # Convert to UTC to calculate the random interval
    start_time_utc = start_time_today_paris.astimezone(pytz.utc)
    end_time_utc = end_time_today_paris.astimezone(pytz.utc)
    time_diff_for_random = end_time_utc - start_time_utc

    for i in range(num_transactions):
        transaction_id = f"TXN_{max_id_num + i + 1:04d}"
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        load_timestamp = datetime.datetime.now(paris_tz).replace(tzinfo=None)

        # Generate a random datetime within the window (Paris Timezone-aware)
        random_seconds = random.uniform(0, time_diff_for_random.total_seconds())
        transaction_datetime_utc_aware = start_time_utc + datetime.timedelta(seconds=random_seconds)
        
        # Convert back to Paris timezone for the record
        transaction_datetime_paris_time = transaction_datetime_utc_aware.astimezone(paris_tz)

        transactions_data.append((
            transaction_id,
            customer_id,
            product_id,
            quantity,
            transaction_datetime_paris_time.replace(tzinfo=None),  # TIMESTAMP_NTZ
            load_timestamp,  # TIMESTAMP_NTZ
            'DEV'  # SOURCE_ENV
        ))

    # Create a Snowpark DataFrame from the generated data
    df = session.create_dataframe(transactions_data, schema=[
        "TRANSACTION_ID", "CUSTOMER_ID", "PRODUCT_ID", "QUANTITY", "TRANSACTION_DATETIME", "LOAD_TIMESTAMP", "SOURCE_ENV"
    ])

    # Write the DataFrame to the Snowflake table
    # Since TRANSACTION_DATETIME and LOAD_TIMESTAMP are timezone-aware datetime objects in Python,
    # Snowpark handles the conversion to TIMESTAMP_TZ seamlessly upon insertion.
    df.write.mode("append").save_as_table(target_table_name)

    return f"Successfully generated and inserted {num_transactions} transactions into {target_table_name} for the period {start_time_today_paris.strftime('%Y-%m-%d %H:%M:%S')} to {end_time_today_paris.strftime('%Y-%m-%d %H:%M:%S')} (Paris time)."
$$;

-- STEP 3
USE ROLE SYSADMIN;
CREATE OR REPLACE TASK RAW_DEV._ORCHESTRATION.TRANSACTION_GENERATION_TASK
  WAREHOUSE = _ORCHESTRATION_DEV_WH
  SCHEDULE = 'USING CRON 0 22 * * * Europe/Paris'
  COMMENT = 'Generate new transactions.'
AS
  CALL RAW_DEV._ORCHESTRATION.GENERATE_X_TRANSACTIONS('RAW_DEV.RAW.TRANSACTIONS', 2);


-- Execute manually for testing
CALL RAW_DEV._ORCHESTRATION.GENERATE_X_TRANSACTIONS('RAW_DEV.RAW.TRANSACTIONS', 3);
SELECT * FROM RAW_DEV.RAW.TRANSACTIONS;

-- Start the task
-- USE ROLE ACCOUNTADMIN;
-- ALTER TASK RAW_DEV._ORCHESTRATION.TRANSACTION_GENERATION_TASK RESUME;

----------------
----------------
-- ADD dew data based on existing ones
USE WAREHOUSE _ORCHESTRATION_PRD_WH;
CREATE OR REPLACE PROCEDURE RAW_PRD._ORCHESTRATION.GENERATE_X_TRANSACTIONS(target_table_name VARCHAR, transaction_count NUMBER)
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
from snowflake.snowpark.functions import lit, col, max as sf_max, substring
from snowflake.snowpark.types import IntegerType
import datetime
import random
import pytz 

def generate_and_insert_transactions(session: Session, target_table_name: str, transaction_count: int):
    num_transactions = transaction_count 

    # --- Fetch existing customer and product IDs for referential integrity ---
    customer_ids = session.table("RAW_PRD.RAW.CUSTOMERS").select(col("CUSTOMER_ID")).collect()
    customer_ids = [row[0] for row in customer_ids]
    if not customer_ids:
        return "Error: No customers found in RAW_PRD.RAW.CUSTOMERS. Cannot generate transactions."

    product_ids = session.table("RAW_PRD.RAW.PRODUCTS").select(col("PRODUCT_ID")).collect()
    product_ids = [row[0] for row in product_ids]
    if not product_ids:
        return "Error: No products found in RAW_PRD.RAW.PRODUCTS. Cannot generate transactions."
    # --- End Fetch ---

    # --- Get max transaction ID for incremental pattern ---
    max_id_result = session.table(target_table_name).select(
        sf_max(substring(col("TRANSACTION_ID"), 5, 10).cast(IntegerType()))
    ).collect()
    max_id_num = max_id_result[0][0] if max_id_result[0][0] is not None else 0

    transactions_data = []

    # Define the Paris timezone
    paris_tz = pytz.timezone("Europe/Paris")

    # Get today's date and time in Paris time
    today_date = datetime.datetime.now(paris_tz).date()
    now_paris = datetime.datetime.now(paris_tz)
    
    # Define the start and end times for transactions on today's date, localized to Paris
    start_time_today_paris = paris_tz.localize(datetime.datetime(today_date.year, today_date.month, today_date.day, 8, 0, 0))
    end_time_today_paris = now_paris

    # Convert to UTC to calculate the random interval
    start_time_utc = start_time_today_paris.astimezone(pytz.utc)
    end_time_utc = end_time_today_paris.astimezone(pytz.utc)
    time_diff_for_random = end_time_utc - start_time_utc

    for i in range(num_transactions):
        transaction_id = f"TXN_{max_id_num + i + 1:04d}"
        customer_id = random.choice(customer_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        load_timestamp = datetime.datetime.now(paris_tz).replace(tzinfo=None)

        # Generate a random datetime within the window (Paris Timezone-aware)
        random_seconds = random.uniform(0, time_diff_for_random.total_seconds())
        transaction_datetime_utc_aware = start_time_utc + datetime.timedelta(seconds=random_seconds)
        
        # Convert back to Paris timezone for the record
        transaction_datetime_paris_time = transaction_datetime_utc_aware.astimezone(paris_tz)

        transactions_data.append((
            transaction_id,
            customer_id,
            product_id,
            quantity,
            transaction_datetime_paris_time.replace(tzinfo=None),  # TIMESTAMP_NTZ
            load_timestamp,  # TIMESTAMP_NTZ
            'PRD'  # SOURCE_ENV
        ))

    # Create a Snowpark DataFrame from the generated data
    df = session.create_dataframe(transactions_data, schema=[
        "TRANSACTION_ID", "CUSTOMER_ID", "PRODUCT_ID", "QUANTITY", "TRANSACTION_DATETIME", "LOAD_TIMESTAMP", "SOURCE_ENV"
    ])

    # Write the DataFrame to the Snowflake table
    # Since TRANSACTION_DATETIME and LOAD_TIMESTAMP are timezone-aware datetime objects in Python,
    # Snowpark handles the conversion to TIMESTAMP_TZ seamlessly upon insertion.
    df.write.mode("append").save_as_table(target_table_name)

    return f"Successfully generated and inserted {num_transactions} transactions into {target_table_name} for the period {start_time_today_paris.strftime('%Y-%m-%d %H:%M:%S')} to {end_time_today_paris.strftime('%Y-%m-%d %H:%M:%S')} (Paris time)."
$$;

-- STEP 3
USE ROLE SYSADMIN;
CREATE OR REPLACE TASK RAW_PRD._ORCHESTRATION.TRANSACTION_GENERATION_TASK
  WAREHOUSE = _ORCHESTRATION_PRD_WH
  SCHEDULE = 'USING CRON 0 22 * * * Europe/Paris'
  COMMENT = 'Generate new transactions.'
AS
  CALL RAW_PRD._ORCHESTRATION.GENERATE_X_TRANSACTIONS('RAW_PRD.RAW.TRANSACTIONS', 2);


-- Execute manually for testing
CALL RAW_PRD._ORCHESTRATION.GENERATE_X_TRANSACTIONS('RAW_PRD.RAW.TRANSACTIONS', 3);
SELECT * FROM RAW_PRD.RAW.TRANSACTIONS;

-- Start the task
-- USE ROLE ACCOUNTADMIN;
-- ALTER TASK RAW_PRD._ORCHESTRATION.TRANSACTION_GENERATION_TASK RESUME;

----------------
