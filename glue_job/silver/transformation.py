import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import uuid
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime, timezone, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsgluedq.transforms import EvaluateDataQuality



args = getResolvedOptions(sys.argv, ['JOB_NAME','target','source','events','stores','utiletl','watermark'])



sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.exchange.reuse", "false")

run_id = str(uuid.uuid4())
run_started_at = datetime.now(timezone.utc)
status = "STARTED"
error_message = None
run_finished_at = None
rows_processed = 0
table_pipeline = [
    {
        "source_view": "stg_events",
        "target_table": "glue_catalog.silver.events",
        "join_condition": "target.event_id = source.event_id",
        "update_set": "*"
    },
    {
        "source_view": "stg_stores",
        "target_table": "glue_catalog.silver.stores",
        "join_condition": "target.store_id = source.store_id",
        "update_set": "*"
    }
]

def read():
    try:
        events_df = spark.table(f"glue_catalog.{args['source']}."f"{args['events']}")
        stores_df = spark.table(f"glue_catalog.{args['source']}."f"{args['stores']}")
        watermark = spark.table(f"glue_catalog.{args['utiletl']}."f"{args['watermark']}")
    except Exception as e:
        raise e
    last_watermark = (watermark.filter(
        (trim(col('target_table')) == 'silver') &
        (lower(col('status')) == 'success')
    ).agg(max('last_processed_watermark').alias('max_wm'))
    .first()['max_wm'])
    if last_watermark is None:        
        last_watermark = '1970-01-01 00:00:00'         
    last_watermark = datetime.fromisoformat(str(last_watermark))    
    lookback_watermark = last_watermark - timedelta(days=3)
    events_df = events_df.filter(col("event_time").cast("timestamp") > lit(lookback_watermark))   
    return events_df,stores_df,watermark

def validate_schema(df,excepted_schema,allow_extra=False):
    actual_feilds = {feild.name : feild.dataType        
    for feild in df.schema.fields }    
    expepted_feilds ={ feild.name : feild.dataType        
    for feild in excepted_schema.fields    }    
    missing = set(expepted_feilds) - set(actual_feilds)    
    extra = set(actual_feilds) - set(expepted_feilds)
    errors=[]
    if missing:
        errors.append(f'missing col: {sorted(missing)}')
    if extra and not allow_extra:
        errors.append(f'extra col: {sorted(extra)}')
    if errors:        
        raise ValueError(";".join(errors))    

def cleaning(events_dyf):    
    ruleset = """ Rules = [IsComplete "event_id",ColumnValues "event_type" in ["shopper_assigned", "placed", "picked","delivered","canceled"],IsComplete "order_id"]
    """    
    dq = EvaluateDataQuality().process_rows(
        frame=events_dyf,   
        ruleset=ruleset,    
        publishing_options={        
            "dataQualityEvaluationContext": "events_dyf",        
            "enableDataQualityCloudWatchMetrics": False,       
            "enableDataQualityResultsPublishing": False,    },   
            additional_options={"performanceTuning.caching": "CACHE_NOTHING"},    )    
    rows = dq["rowLevelOutcomes"].toDF()    
    valid_df = rows.filter("dataQualityEvaluationResult = 'Passed'")    
    qurantine = rows.filter("dataQualityEvaluationResult = 'Failed'")    
    valid_df = valid_df.dropDuplicates(["event_id"])    
    return valid_df,qurantine

def writing(valid_df,stores_df,qurantine,run_started_at,status,run_id,error_message,rows_processed,table_pipeline):    
    dq_cols = ["DataQualityRulesPass", "DataQualityRulesFail","DataQualityRulesSkip", "DataQualityEvaluationResult"]    
    valid_df = valid_df.drop(*dq_cols)   
    log_add = ( f"glue_catalog.{args['utiletl']}."f"{args['watermark']}")
    valid_df = valid_df.localCheckpoint()
    stores_df = stores_df.localCheckpoint()
    valid_df.createOrReplaceTempView("stg_events")
    stores_df.createOrReplaceTempView("stg_stores")
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
            rows_processed =events_df.count()
            status='success'
        except Exception as e:
            print(f"Error merging {src} into {tgt}: {str(e)}")
            status = "FAILED"        
            error_message = str(e)[:4000]
            raise e
        finally:        
            run_finished_at = datetime.now(timezone.utc)
    spark.catalog.clearCache()
    qurantine.write.mode("append").format("parquet").save("s3://s3-silver-data-lake-project/quarantine/")
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
      StructField("last_processed_watermark",TimestampType(), True),])
    last_processed_watermark = (valid_df.agg( max(col("event_time").cast("timestamp")).alias("max_wm")).first()["max_wm"] )    
    log_data = [('bronze', 'silver',datetime.now(timezone.utc),run_started_at,run_finished_at,status,rows_processed,run_id,error_message,last_processed_watermark)]    
    log_df = spark.createDataFrame(log_data, schema=log_schema)    
    log_df.writeTo(log_add).append()
    
events_df,stores_df,watermark = read()
if events_df.limit(1).count()==0:
    print('no new data')
    job.commit()
else:
    excepted_schema = StructType([    
        StructField('event_id',LongType(),False),    
        StructField('order_id',LongType(),True),    
        StructField('store_id',LongType(),True),    
        StructField('event_type',StringType(),True),    
        StructField('event_time',StringType(),True),    
        StructField(('ingested_time'),TimestampType(),True)    ])    
    validate_schema(events_df,excepted_schema)    
    events_dyf = DynamicFrame.fromDF(events_df,glueContext,'events_dyf')    
    valid_df,quarantine = cleaning(events_dyf)    
    writing(valid_df,stores_df,quarantine,run_started_at,status,run_id,error_message,rows_processed,table_pipeline)
job.commit()