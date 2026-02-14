{{ config(materialized='view') }}

SELECT
v:id:string AS transaction_id ,
v:account_id::string as account_id,
v:amount::float AS amount,
v:currency::string AS currency,
v:transaction_date::timestamp AS transaction_date,
v:related_account_id::string AS related_account_id,
v:status::string AS status,
v:created_at::timestamp AS timestamp_time,
CURRENT_TIMESTAMP() AS load_timestamp
from {{source('raw', 'transactions')}} 
