from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
checkpoint = "file:////Users/kazdy/workspace/HUDI-8259/checkpoint/2"
path = "file:////Users/kazdy/workspace/HUDI-8259/testtable"

hudi_options = {
    "hoodie.datasource.write.recordkey.field": "col27,col7",
    "hoodie.datasource.write.partitionpath.field": "partitionCol1",
    "hoodie.datasource.write.table.name": "testtable",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.precombine.field": "col4",
    "hoodie.upsert.shuffle.parallelism": "2",
    "hoodie.insert.shuffle.parallelism": "2",
    "hoodie.bulkinsert.shuffle.parallelism": "2",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.table.name": "testtable",
}

spark = SparkSession \
        .builder \
        .master("local[1]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.0") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

# RUN BELOW AS SEPARATE SNIPPET TO TRIGGER ERROR
# replace path with file that was created by clustering
from schema import schema

df = spark.read.format("parquet").schema(schema).load("upserts_data/2023/3") # we need partial update, it does not throw errors for full file overwrite
df.printSchema()

df.write.format("hudi").mode("append").options(**hudi_options).save(path) # this should throw error, not able to update records in clustered file 

spark.stop()