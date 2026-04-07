{{ config(materialized='view') }}

SELECT
    $1::STRING AS col1,
    $2::STRING AS col2,
    $3::STRING AS col3
FROM @RAW_DEV.RAW.MY_TEST_STAGE/data.csv
(FILE_FORMAT => RAW_DEV.RAW.CSV_FORMAT)