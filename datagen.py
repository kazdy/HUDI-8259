from mimesis import Datetime
from mimesis import Text
from mimesis.random import Random
from mimesis import Numbers
import uuid
import itertools
from pyspark.sql import SparkSession

from pyspark.sql.functions import lit, when
from schema import schema

mtext = Text()
mdatetime = Datetime()
mrand = Random()
mnumbers = Numbers()


spark = SparkSession.builder.master("local[1]").config("spark.driver.memory", "8g").getOrCreate()

def pyspark_df(funs, columns=None, num_rows=1, spark=spark):
    def functions():
        return tuple(map(lambda f: f(), funs))
    if not columns:
        columns = list(map(lambda f: f.__name__, funs))
    data = []
    for _ in itertools.repeat(None, num_rows):
        data.append(functions())
    return spark.createDataFrame(data, columns)

def get_datagen(year, month):
    datagen = [
    mrand.randstr,
    mrand.randstr,
    lambda: mrand.randint(1,2000000),
    lambda: mrand.randint(1,2000000),
    mdatetime.datetime,
    lambda: year,
    lambda: month,
    mrand.randstr,
    mdatetime.datetime,
    lambda: mrand.randint(1,2000000),
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    lambda: mnumbers.decimal_number(1.0,2000000.0),
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    lambda: mrand.randint(1,2000000),
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    lambda: mrand.randint(1,2000000),
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    mrand.randstr,
    lambda: mrand.randint(1,2000000),
    lambda: mrand.randint(1,2000000),
    mrand.randstr,
    mrand.randstr,
    lambda: str(uuid.uuid4()),
    mrand.randstr,
    mrand.randstr,
    mrand.randstr
    ]
    return datagen

## 100k rows = 23MB
## 800k rows = 200MB
def generate(schema, datagen, year, month, dir_name="input_data"):
    df = pyspark_df(
                    datagen(year, month),
                    schema, 
                    100,
                    spark)
    df.write.format("parquet").mode("overwrite").option("compression", "gzip").save(f"{dir_name}/{year}/{month}")

generate(schema, get_datagen, 2022, 11, dir_name="input_data")
generate(schema, get_datagen, 2022, 12, dir_name="input_data")
generate(schema, get_datagen, 2023, 2,  dir_name="input_data")
generate(schema, get_datagen, 2023, 3, dir_name="input_data")
generate(schema, get_datagen, 2023, 3, dir_name="inserts_data")

df_inserts = spark.read.parquet("inserts_data/2023/3")
df_updates = spark.read.parquet("input_data/2023/3").limit(10).withColumn("col4", when(lit(True),lit(2).cast('int')))
df_upserts = df_inserts.union(df_updates)
df_upserts.write.format("parquet").mode("append").option("compression", "gzip").save(f"upserts_data/2023/3")

assert df_updates.schema == schema

spark.stop()
