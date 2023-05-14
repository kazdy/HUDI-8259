from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

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
from schema import schema
# separate writes as we want to create a few commits to test clustering and rollback to instant
df = spark.read.format("parquet").schema(schema).load("input_data/2022/11")
df.write.format("hudi").mode("append").options(**hudi_options).save(path)

df = spark.read.format("parquet").schema(schema).load("input_data/2022/12")
df.write.format("hudi").mode("append").options(**hudi_options).save(path)

df = spark.read.format("parquet").schema(schema).load("input_data/2023/2")
df.write.format("hudi").mode("append").options(**hudi_options).save(path)

df = spark.read.format("parquet").schema(schema).load("input_data/2023/3")
df.write.format("hudi").mode("append").options(**hudi_options).save(path)

# assert spark.read.format("hudi").load("testtable/*").count() == 800000 * 4

# # run clustering
path_target = "file:////Users/kazdy/workspace/HUDI-8259/testtable"

df = spark.sql(f"CALL run_clustering(path => '{path_target}');")
df.show()
clustering_time = df.select("timestamp").collect()[0][0]
print(clustering_time)
# # 20230323184211330
