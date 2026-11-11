import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','source','target','events','stores'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
table_pipeline = [
    {
        "source_view": "stg_fct_events",
        "target_table": "glue_catalog.gold.fct_events",
        "join_condition": "target.event_id = source.event_id",
        "update_set": "*"
    }
]

def reading():
    events_df = spark.table(f"glue_catalog.{args['source']}."f"{args['events']}")
    stores_df = spark.table(f"glue_catalog.{args['source']}."f"{args['stores']}")
    return events_df,stores_df
def prepare(events_df):
    event = events_df.withColumn('etkey',md5(col('event_type')))
    event = event.withColumn('skey',md5(col('store_id').cast("string")))
    event = (
    event
    .withColumn("event_time", to_timestamp(col("event_time")).cast("timestamp_ntz"))
    .withColumn("event_date_key", date_format(col("event_time"), "yyyyMMdd").cast("int"))
)
    fct_events = event.select('event_id','order_id','skey','etkey','event_date_key','event_time','ingested_time')
    fct_events.show(1)
    return fct_events
def writing(fct_events,table_pipeline):
    fct_events = fct_events.localCheckpoint()
    fct_events.createOrReplaceTempView("stg_fct_events")
    for pipeline in table_pipeline:
        src = pipeline["source_view"]
        tgt = pipeline["target_table"]
        join_cond = pipeline["join_condition"]
        up_set = pipeline["update_set"]
        
        print(f"Starting MERGE process from {src} into {tgt}...")
        merge_sql = f"""
        MERGE INTO {tgt} AS target
        USING {src} AS source
        ON {join_cond}
        WHEN MATCHED THEN
          UPDATE SET {up_set}
        WHEN NOT MATCHED THEN
          INSERT *
         """
        try:
            spark.sql(merge_sql)
            print(f"Successfully merged {src} into {tgt}.")
            spark.catalog.clearCache()
        except Exception as e:
            print(f"Error merging {src} into {tgt}: {str(e)}")
            raise e

events_df,stores_df = reading()
if events_df.limit(1).count()==0:
    print('no new data')
    job.commit()
else:
    fct_events = prepare(events_df)
    writing(fct_events,table_pipeline)
    job.commit()