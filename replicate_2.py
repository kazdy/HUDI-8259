from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

path = "file:////Users/kazdy/workspace/hudi-clustering/testtable"
hudi_options = {
    "hoodie.datasource.write.recordkey.field": "a_id",
    "hoodie.datasource.write.partitionpath.field": "year",
    "hoodie.datasource.write.table.name": "testtable",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.precombine.field": "pcf",
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
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.1") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

# RUN BELOW AS SEPARATE SNIPPET TO TRIGGER ERROR
# replace path with file that was created by clustering
df = spark.read.parquet("testtable/year=2022/e09e6824-65b2-46df-ad78-e77561bb69ea-0_0-156-179_20230323184211330.parquet").limit(1000) # we need partial update, it does not throw errors for full file overwrite
df.show()
df.printSchema()
df = df.withColumn("pcf", lit(2)) # simulate updates
# return to original record schema
df = df.select("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name", "a_id", "a_column", "b_column", "c_column", "year", "month", "pcf", "header")
df.show()
df.printSchema()
df.write.format("hudi").mode("append").options(**hudi_options).save(path) # this will throw error, not able to update records in clustered file 
