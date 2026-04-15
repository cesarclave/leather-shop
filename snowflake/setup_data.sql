-- CREATE TABLES AND ADD DATA
USE ROLE SYSADMIN;
USE SCHEMA RAW_DEV.RAW;

--- 1. CUSTOMERS ---
CREATE OR REPLACE TABLE CUSTOMERS AS
SELECT 
    column1::VARCHAR(200) as cust_number,
    column2::VARCHAR(200) as fullname,
    column3::VARCHAR(200) as email_address,
    column4::VARCHAR(200) as city,
    column5::VARCHAR(200) as country,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP()))) as updated_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'DEV'::VARCHAR(200) as src
FROM VALUES
    ('CUST_001', 'Jean Dupont', 'jean.dupont@email.com', 'Paris', 'France', -5),
    ('CUST_002', 'Marie Dubois', 'marie.dubois@email.com', 'Marseille', 'France', -4),
    ('CUST_003', 'Pierre Martin', 'pierre.martin@email.com', 'Lyon', 'France', -3),
    ('CUST_004', 'Sophie Bernard', 'sophie.bernard@email.com', 'Toulouse', 'France', -2),
    ('CUST_005', 'Lucie Petit', 'lucie.petit@email.com', 'Nice', 'France', -1);

--- 2. PRODUCTS ---
CREATE OR REPLACE TABLE PRODUCTS AS
SELECT 
    column1::VARCHAR(200) as sku,
    column2::VARCHAR(200) as item_desc,
    column3::VARCHAR(200) as category,
    column4::VARCHAR(200) as material_type,
    column5 as unit_price,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP()))) as updated_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'DEV'::VARCHAR(200) as src
FROM VALUES
    ('PROD_001', 'Sac à main bandoulière', 'Sacs', 'Cuir', 350.00, -1),
    ('PROD_002', 'Portefeuille compagnon', 'Portefeuilles', 'Cuir', 120.00, -3),
    ('PROD_003', 'Ceinture réversible', 'Ceintures', 'Cuir', 85.50, -2),
    ('PROD_004', 'Sac à dos urbain', 'Sacs', 'Toile', 480.00, -4),
    ('PROD_005', 'Porte-cartes minimaliste', 'Portefeuilles', 'Toile', 60.00, -5);

--- 3. ORDERS ---
CREATE OR REPLACE TABLE ORDERS AS
SELECT 
    column1::VARCHAR(200) as order_ref,
    column2::VARCHAR(200) as cust_number,
    column3::VARCHAR(200) as sku,
    column4 as qty,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column5, CURRENT_TIMESTAMP()))) as ordered_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'DEV'::VARCHAR(200) as src
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
    column1::VARCHAR(200) as cust_number,
    column2::VARCHAR(200) as fullname,
    column3::VARCHAR(200) as email_address,
    column4::VARCHAR(200) as city,
    column5::VARCHAR(200) as country,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP()))) as updated_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'PRD'::VARCHAR(200) as src
FROM VALUES
    ('CUST_101', 'Alice Morel', 'alice.morel@email.com', 'Bordeaux', 'France', -10),
    ('CUST_102', 'Thomas Roux', 'thomas.roux@email.com', 'Nantes', 'France', -8),
    ('CUST_103', 'Emma Lefebvre', 'emma.lefebvre@email.com', 'Lille', 'France', -6),
    ('CUST_104', 'Nicolas Petit', 'nicolas.petit@email.com', 'Strasbourg', 'France', -4),
    ('CUST_105', 'Chloe Garcia', 'chloe.garcia@email.com', 'Montpellier', 'France', -2);

--- 2. PRODUCTS ---
CREATE OR REPLACE TABLE PRODUCTS AS
SELECT 
    column1::VARCHAR(200) as sku,
    column2::VARCHAR(200) as item_desc,
    column3::VARCHAR(200) as category,
    column4::VARCHAR(200) as material_type,
    column5 as unit_price,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column6, CURRENT_TIMESTAMP()))) as updated_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'PRD'::VARCHAR(200) as src
FROM VALUES
    ('PROD_001', 'Sac à main bandoulière', 'Sacs', 'Cuir', 350.00, -10),
    ('PROD_002', 'Portefeuille compagnon', 'Portefeuilles', 'Cuir', 120.00, -20),
    ('PROD_003', 'Ceinture réversible', 'Ceintures', 'Cuir', 85.50, -15),
    ('PROD_004', 'Sac à dos urbain', 'Sacs', 'Toile', 480.00, -30),
    ('PROD_005', 'Porte-cartes minimaliste', 'Portefeuilles', 'Toile', 60.00, -45);

--- 3. ORDERS ---
CREATE OR REPLACE TABLE ORDERS AS
SELECT 
    column1::VARCHAR(200) as order_ref,
    column2::VARCHAR(200) as cust_number,
    column3::VARCHAR(200) as sku,
    column4 as qty,
    DATEADD(second, UNIFORM(0, 59, RANDOM()), DATEADD(millisecond, UNIFORM(0, 999, RANDOM()), DATEADD(minute, column5, CURRENT_TIMESTAMP()))) as ordered_at,
    CURRENT_TIMESTAMP() as _loaded_at,
    'PRD'::VARCHAR(200) as src
FROM VALUES
    ('ORD_2001', 'CUST_101', 'PROD_001', 1, -5760),
    ('ORD_2002', 'CUST_102', 'PROD_002', 1, -4320),
    ('ORD_2003', 'CUST_103', 'PROD_003', 2, -2880),
    ('ORD_2004', 'CUST_101', 'PROD_004', 1, -1440),
    ('ORD_2005', 'CUST_105', 'PROD_005', 5, -60);