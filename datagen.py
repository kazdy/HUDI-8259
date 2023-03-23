# a_id
# a_column
# b_column
# c_column
# year
# month
# header


from mimesis import Datetime
from mimesis import Text
from mimesis.random import Random
import itertools
from pyspark.sql import SparkSession


mtext = Text()
mdatetime = Datetime()
mrand = Random()
spark = SparkSession.builder.master("local[1]").config("spark.driver.memory", "8g").getOrCreate()
def pyspark_df(funs, columns=None, num_rows=1, spark = SparkSession.builder.master("local[2]").config("spark.driver.memory", "12g").getOrCreate()):
    def functions():
        return tuple(map(lambda f: f(), funs))
    if not columns:
        columns = list(map(lambda f: f.__name__, funs))
    data = []
    for _ in itertools.repeat(None, num_rows):
        data.append(functions())
    return spark.createDataFrame(data, columns)

## 100k rows = 23MB
## 800k rows = 200MB
def generate(year, month):
    df2022_11 = pyspark_df([mrand.randstr, mtext.text, mtext.text, mtext.text, lambda: year, lambda: month, lambda: 1, mtext.text], 
                    ['a_id', 'a_column', 'b_column', 'c_column', 'year', 'month', 'pcf' ,'header'], 
                    800000,
                    spark)
    df2022_11.write.format("parquet").mode("overwrite").option("compression", "gzip").save(f"input_data/{year}/{month}")

# had to do it one by one because of memory issues
generate(2022, 11)
generate(2022, 12)
generate(2023, 2)
generate(2023, 3)

spark.stop()
