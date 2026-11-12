import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import uuid
from datetime import datetime, timezone, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','target','source','events','stores','utiletl','watermark'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

load_store =1
run_id = str(uuid.uuid4())
run_started_at = datetime.now(timezone.utc)
status = "STARTED"
error_message = None
run_finished_at = None
rows_processed = 0

def read():
    try:
        events_df = glueContext.create_dynamic_frame.from_catalog(
        database=args['source'],
        table_name=args['events']
        ).toDF()
        stores_df = glueContext.create_dynamic_frame.from_catalog(
        database = args['source'],
        table_name = args['stores']
        ).toDF()
        watermark = spark.table(f"glue_catalog.{args['utiletl']}."f"{args['watermark']}")
    except Exception as e:
        raise e
    last_watermark = (watermark.filter(
        (trim(col('target_table')) == 'bronze') &
        (lower(col('status')) == 'success')
    ).agg(max('last_watermark').alias('max_wm'))
    .first()['max_wm'])
    if last_watermark is None:
        last_watermark = '1970-01-01 00:00:00'
    events_df = events_df.filter(col('event_time').cast("timestamp")>lit(last_watermark).cast("timestamp"))
    return events_df,stores_df
def writing(events_df,stores_df,status,run_started_at,rows_processed,run_finished_at,run_id,error_message,load_store):
    events_target = ( f"glue_catalog.{args['target']}."f"{args['events']}") 
    stores_target = ( f"glue_catalog.{args['target']}."f"{args['stores']}")
    log_add = ( f"glue_catalog.{args['utiletl']}."f"{args['watermark']}")
    events_df = events_df.withColumn('ingested_time',current_timestamp())
    existing = {f.name.lower() for f in spark.table(events_target).schema.fields}
    for f in events_df.schema.fields:
        if f.name.lower() not in existing:
            spark.sql(f"ALTER TABLE {events_target} ADD COLUMN `{f.name}` {f.dataType.simpleString()}")
    try:
        rows_processed = events_df.count()
        if rows_processed > 0:
            events_df.writeTo(events_target).append()
            status='success'
        else:
            rows_processed=0
        if load_store == 1:
            stores_df.writeTo(stores_target).append()
    except Exception as e:
        status = "FAILED"
        error_message = str(e)[:4000]
    finally:
        run_finished_at = datetime.now(timezone.utc)
    log_schema = StructType([
        StructField("source_table", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("last_watermark", TimestampType(), True),
        StructField("run_started_at", TimestampType(), True),
        StructField("run_finished_at", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("rows_processed", LongType(), True),
        StructField("run_id", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("last_processed_watermark",TimestampType(), True),
    ])
    last_watermark = events_df.agg(
    max(col("event_time").cast("timestamp")).alias("max_t")
    ).collect()[0]["max_t"]
    log_data = [(
    'source',
    'bronze',
    datetime.now(timezone.utc),
    run_started_at,
    run_finished_at,
    status,
    rows_processed,
    run_id,
    error_message,
    None
    )]
    log_df = spark.createDataFrame(log_data, schema=log_schema)
    log_df.writeTo(log_add).append()
events_df,stores_df = read()
if events_df.limit(1).count() == 0:
    print('no new data')
    job.commit()
else:
    writing(events_df,stores_df,status,run_started_at,rows_processed,run_finished_at,run_id,error_message,load_store)
    job.commit()