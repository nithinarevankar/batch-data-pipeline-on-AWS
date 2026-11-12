--bronze event table

CREATE TABLE bronze.events (
  event_id bigint,
  order_id bigint,
  store_id bigint,
  event_type string,
  event_time string,
  ingested_time timestamp,
  app_version string)
LOCATION 's3://bronze-s3-data-bucket/events'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd'
);

--bronze store table

CREATE TABLE bronze.stores (
  store_id string,
  store_name string,
  city string)
LOCATION 's3://bronze-s3-data-bucket/stores'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd'
);

--silver events

CREATE TABLE silver.events (
  event_id bigint,
  order_id bigint,
  store_id bigint,
  event_type string,
  event_time string,
  ingested_time timestamp)
PARTITIONED BY (`event_time`)
LOCATION 's3://s3-silver-data-lake-project/events'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd'
);

--silver stores

CREATE TABLE silver.stores (
  store_id string,
  store_name string,
  city string)
LOCATION 's3://s3-silver-data-lake-project/stores'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd'
);

--gold dim_date

CREATE TABLE gold.dim_date (
  date_key int,
  full_date date,
  calendar_year int,
  quarter_num int,
  month_num int,
  month_name string,
  day_of_month int,
  weekday_num int,
  weekday_name string,
  week_of_year int,
  is_weekend boolean)
LOCATION 's3://gold-s3-datalake/dim_date'
TBLPROPERTIES (
  'table_type'='iceberg',
  'format'='parquet',
  'write_compression'='zstd'
);

--gold dim_stores

CREATE TABLE gold.dim_stores (
  skey string,
  store_id string,
  store_name string,
  city string)
LOCATION 's3://gold-s3-datalake/dim_stores'
TBLPROPERTIES (
  'table_type'='iceberg',
  'format'='parquet',
  'write_compression'='zstd'
);

-- gold dim_events_type

CREATE TABLE gold.dim_events_type (
  etkey string,
  event_type string)
LOCATION 's3://gold-s3-datalake/dim_event_types'
TBLPROPERTIES (
  'table_type'='iceberg',
  'format'='parquet',
  'write_compression'='zstd'
);

--gold fct_events

CREATE TABLE gold.fct_events (
  event_id bigint,
  order_id bigint,
  skey string,
  etkey string,
  event_date_key int,
  event_time timestamp,
  ingested_time timestamp)
PARTITIONED BY (day(`event_time`))
LOCATION 's3://gold-s3-datalake/fact_events'
TBLPROPERTIES (
  'table_type'='iceberg',
  'format'='parquet',
  'write_compression'='zstd'
);

-- watermarlog

CREATE TABLE etl_data.watermarklog (
  source_table string,
  target_table string,
  last_watermark timestamp,
  run_started_at timestamp,
  run_finished_at timestamp,
  status string,
  rows_processed bigint,
  run_id string,
  error_message string,
  last_processed_watermark timestamp)
LOCATION 's3://etl-util-bucket/metadatalog'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='zstd'
);