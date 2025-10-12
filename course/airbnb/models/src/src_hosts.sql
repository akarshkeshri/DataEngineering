-- AIRBNB.raw.raw_hosts     (when abstraction layer sources.yml is not use than use directly table name)
with raw_hosts as (select * from  {{ source('airbnb','hosts') }}
)
select id as host_id,
name as host_name,
is_superhost,
updated_at,
created_at from raw_hosts