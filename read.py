from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

path = "file:////Users/kazdy/workspace/hudi-clustering/testtable"

spark = SparkSession \
        .builder \
        .master("local[1]") \
        .config("spark.driver.memory", "8g") \
        .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.1") \
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

df = spark.read.format("hudi").load(path).where("partitionCol1 = 2023")
df.printSchema()
df.show()


# note that wehen reading partitionCol1 is at the end of DF schema, but the commit schema says it's in the middle, why is file schema prefered?
