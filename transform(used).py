from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType, DateType, FloatType, IntegerType
from pyspark.sql.functions import to_date

S3_DATA_SOURCE_PATH = 'gs://gcp_pipelines/input_files/superstore_dataset.csv'
S3_DATA_OUTPUT_PATH = 'gs://gcp_pipelines/output_files/'

# # Define the schema
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_date", StringType(), False),
    StructField("ship_date", StringType(), False),
    StructField("customer", StringType(), True),
    StructField("manufactory", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("category", StringType(), True),
    StructField("subcategory", StringType(), True),
    StructField("region", StringType(), True),
    StructField("zip", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("discount", FloatType(), True),
    StructField("profit", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sales", FloatType(), True),
    StructField("profit_margin", FloatType(), True)
])

def func_run():
    spark = SparkSession.builder.appName('airflow_with_emr').getOrCreate()

    # all_data = spark.read.option("header", "true").schema(schema).csv(S3_DATA_SOURCE_PATH)
    all_data = spark.read \
        .option("header", "true") \
        .option("dateFormat", "M/d/yyyy") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("multiLine", "true") \
        .option("inferSchema", "false") \
        .schema(schema) \
        .csv(S3_DATA_SOURCE_PATH)

    # Convert order_date and ship_date to proper date format
    all_data = all_data.withColumn("order_date", to_date("order_date", "M/d/yyyy")) \
                       .withColumn("ship_date", to_date("ship_date", "M/d/yyyy"))
    
    selected_data = all_data.select(
        'order_id', 'order_date', 'ship_date', 'customer', 'manufactory', 'product_name', 
        'segment', 'category', 'subcategory', 'state', 'country', 'discount', 
        'profit', 'quantity', 'sales', 'profit_margin'
    )
    
    selected_data.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH)
    
    print('Total number of records: %s' % all_data.count())

if __name__ == "__main__":
    func_run()
