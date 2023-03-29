from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

schema = StructType(
    [StructField('col1', StringType()),
    StructField('col2', StringType()),
    StructField('col3', IntegerType()),
    StructField('col4', IntegerType()), # precombine field
    StructField('col5', TimestampType()),
    StructField('partitionCol1', IntegerType()),
    StructField('partitionCol2', IntegerType()),
    StructField('col6', StringType()),
    StructField('col7', TimestampType()),# PK2
    StructField('col8', IntegerType()),
    StructField('col9', StringType()),
    StructField('col10', StringType()),
    StructField('col11', StringType()),
    StructField('col12', StringType()),
    StructField('col13', DecimalType()),
    StructField('col14', StringType()),
    StructField('col15', StringType()),
    StructField('col16', StringType()),
    StructField('col17', StringType()),
    StructField('col18', StringType()),
    StructField('col19', StringType()),
    StructField('col20', StringType()),
    StructField('col21', IntegerType()),
    StructField('col22', StringType()),
    StructField('col23', StringType()),
    StructField('col24', StringType()),
    StructField('col25', IntegerType()),
    StructField('col26', StringType()),
    StructField('col27', StringType()),
    StructField('col28', StringType()),
    StructField('col29', StringType()),
    StructField('col30', StringType()),
    StructField('col31', StringType()),
    StructField('col32', StringType()),
    StructField('col33', IntegerType()),
    StructField('col34', IntegerType()),
    StructField('col35', StringType()),
    StructField('col36', StringType()),
    StructField('col37', StringType()), # PK
    StructField('col38', StringType()),
    StructField('col39', StringType()),
    StructField('col40', StringType())
    ]
)