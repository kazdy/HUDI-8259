## How to replicate the error.

install dependencies  
`pip install -r requirements.txt`  
run datagen.py to generate input files  
`python3 datagen.py`  
run replicate_1.py to create table with data and cluster it  
`python3 replicate_1.py`  
run replicate_2.py to replicate the excetion  
`python3 replicate_2.py`  

I'm attaching all the data I used to create the example.

I tried a few times and it always fails with the same error.

```
➜  hudi-clustering git:(replicated) ✗ /usr/local/bin/python3 /Users/kazdy/workspace/hudi-clustering/replicate_2.py
23/03/29 22:46:02 WARN Utils: Your hostname, Daniels-MacBook-Air.local resolves to a loopback address: 127.0.0.1; using 192.168.0.184 instead (on interface en0)
23/03/29 22:46:02 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
:: loading settings :: url = jar:file:/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/site-packages/pyspark/jars/ivy-2.5.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
Ivy Default Cache set to: /Users/kazdy/.ivy2/cache
The jars for the packages stored in: /Users/kazdy/.ivy2/jars
org.apache.hudi#hudi-spark3.3-bundle_2.12 added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-279c5bb5-7ed3-421a-8483-a1826d66d224;1.0
        confs: [default]
        found org.apache.hudi#hudi-spark3.3-bundle_2.12;0.12.1 in central
:: resolution report :: resolve 68ms :: artifacts dl 2ms
        :: modules in use:
        org.apache.hudi#hudi-spark3.3-bundle_2.12;0.12.1 from central in [default]
        ---------------------------------------------------------------------
        |                  |            modules            ||   artifacts   |
        |       conf       | number| search|dwnlded|evicted|| number|dwnlded|
        ---------------------------------------------------------------------
        |      default     |   1   |   0   |   0   |   0   ||   1   |   0   |
        ---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-279c5bb5-7ed3-421a-8483-a1826d66d224
        confs: [default]
        0 artifacts copied, 1 already retrieved (0kB/2ms)
23/03/29 22:46:02 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
root
 |-- col1: string (nullable = true)
 |-- col2: string (nullable = true)
 |-- col3: integer (nullable = true)
 |-- col4: integer (nullable = true)
 |-- col5: timestamp (nullable = true)
 |-- partitionCol1: integer (nullable = true)
 |-- partitionCol2: integer (nullable = true)
 |-- col6: string (nullable = true)
 |-- col7: timestamp (nullable = true)
 |-- col8: integer (nullable = true)
 |-- col9: string (nullable = true)
 |-- col10: string (nullable = true)
 |-- col11: string (nullable = true)
 |-- col12: string (nullable = true)
 |-- col13: decimal(10,0) (nullable = true)
 |-- col14: string (nullable = true)
 |-- col15: string (nullable = true)
 |-- col16: string (nullable = true)
 |-- col17: string (nullable = true)
 |-- col18: string (nullable = true)
 |-- col19: string (nullable = true)
 |-- col20: string (nullable = true)
 |-- col21: integer (nullable = true)
 |-- col22: string (nullable = true)
 |-- col23: string (nullable = true)
 |-- col24: string (nullable = true)
 |-- col25: integer (nullable = true)
 |-- col26: string (nullable = true)
 |-- col27: string (nullable = true)
 |-- col28: string (nullable = true)
 |-- col29: string (nullable = true)
 |-- col30: string (nullable = true)
 |-- col31: string (nullable = true)
 |-- col32: string (nullable = true)
 |-- col33: integer (nullable = true)
 |-- col34: integer (nullable = true)
 |-- col35: string (nullable = true)
 |-- col36: string (nullable = true)
 |-- col37: string (nullable = true)
 |-- col38: string (nullable = true)
 |-- col39: string (nullable = true)
 |-- col40: string (nullable = true)

23/03/29 22:46:05 WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
23/03/29 22:46:05 WARN DFSPropertiesConfiguration: Cannot find HUDI_CONF_DIR, please set it as the dir of hudi-defaults.conf
23/03/29 22:46:05 WARN DFSPropertiesConfiguration: Properties file file:/etc/hudi/conf/hudi-defaults.conf not found. Ignoring to load props file
# WARNING: Unable to attach Serviceability Agent. Unable to attach even with module exceptions: [org.apache.hudi.org.openjdk.jol.vm.sa.SASupportException: Sense failed., org.apache.hudi.org.openjdk.jol.vm.sa.SASupportException: Sense failed., org.apache.hudi.org.openjdk.jol.vm.sa.SASupportException: Sense failed.]
23/03/29 22:46:08 WARN MetricsConfig: Cannot locate configuration: tried hadoop-metrics2-hbase.properties,hadoop-metrics2.properties
23/03/29 22:46:09 ERROR BoundedInMemoryExecutor: error producing records        
org.apache.hudi.exception.HoodieException: unable to read next record from parquet file 
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:53)
        at org.apache.hudi.common.util.queue.IteratorBasedQueueProducer.produce(IteratorBasedQueueProducer.java:45)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$0(BoundedInMemoryExecutor.java:106)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:750)
Caused by: org.apache.parquet.io.ParquetDecodingException: Can not read value at 1 in block 0 in file file:/Users/kazdy/workspace/hudi-clustering/testtable/partitionCol1=2023/e2f39627-252a-416b-a9d4-1e0c21d10df6-0_0-148-113_20230329224554584.parquet
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:254)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:132)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:136)
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:48)
        ... 8 more
Caused by: java.lang.UnsupportedOperationException: org.apache.parquet.avro.AvroConverters$FieldFixedConverter
        at org.apache.parquet.io.api.PrimitiveConverter.addLong(PrimitiveConverter.java:105)
        at org.apache.parquet.column.impl.ColumnReaderBase$2$4.writeValue(ColumnReaderBase.java:325)
        at org.apache.parquet.column.impl.ColumnReaderBase.writeCurrentValueToConverter(ColumnReaderBase.java:440)
        at org.apache.parquet.column.impl.ColumnReaderImpl.writeCurrentValueToConverter(ColumnReaderImpl.java:30)
        at org.apache.parquet.io.RecordReaderImplementation.read(RecordReaderImplementation.java:406)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:229)
        ... 11 more
23/03/29 22:46:10 ERROR BoundedInMemoryExecutor: error consuming records 1) / 1]
org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.throwExceptionIfFailed(BoundedInMemoryQueue.java:248)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.readNextRecord(BoundedInMemoryQueue.java:226)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.access$100(BoundedInMemoryQueue.java:52)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue$QueueIterator.hasNext(BoundedInMemoryQueue.java:278)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer.consume(BoundedInMemoryQueueConsumer.java:36)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$2(BoundedInMemoryExecutor.java:135)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:750)
Caused by: org.apache.hudi.exception.HoodieException: unable to read next record from parquet file 
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:53)
        at org.apache.hudi.common.util.queue.IteratorBasedQueueProducer.produce(IteratorBasedQueueProducer.java:45)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$0(BoundedInMemoryExecutor.java:106)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
        ... 4 more
Caused by: org.apache.parquet.io.ParquetDecodingException: Can not read value at 1 in block 0 in file file:/Users/kazdy/workspace/hudi-clustering/testtable/partitionCol1=2023/e2f39627-252a-416b-a9d4-1e0c21d10df6-0_0-148-113_20230329224554584.parquet
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:254)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:132)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:136)
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:48)
        ... 8 more
Caused by: java.lang.UnsupportedOperationException: org.apache.parquet.avro.AvroConverters$FieldFixedConverter
        at org.apache.parquet.io.api.PrimitiveConverter.addLong(PrimitiveConverter.java:105)
        at org.apache.parquet.column.impl.ColumnReaderBase$2$4.writeValue(ColumnReaderBase.java:325)
        at org.apache.parquet.column.impl.ColumnReaderBase.writeCurrentValueToConverter(ColumnReaderBase.java:440)
        at org.apache.parquet.column.impl.ColumnReaderImpl.writeCurrentValueToConverter(ColumnReaderImpl.java:30)
        at org.apache.parquet.io.RecordReaderImplementation.read(RecordReaderImplementation.java:406)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:229)
        ... 11 more
23/03/29 22:46:11 ERROR BaseSparkCommitActionExecutor: Error upserting bucketType UPDATE for partition :0
org.apache.hudi.exception.HoodieException: org.apache.hudi.exception.HoodieException: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:149)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdateInternal(BaseSparkCommitActionExecutor.java:358)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdate(BaseSparkCommitActionExecutor.java:349)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpsertPartition(BaseSparkCommitActionExecutor.java:322)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.lambda$mapPartitionsAsRDD$a3ab3c4$1(BaseSparkCommitActionExecutor.java:244)
        at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1(JavaRDDLike.scala:102)
        at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1$adapted(JavaRDDLike.scala:102)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2(RDD.scala:907)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2$adapted(RDD.scala:907)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.$anonfun$getOrCompute$1(RDD.scala:378)
        at org.apache.spark.storage.BlockManager.$anonfun$doPutIterator$1(BlockManager.scala:1535)
        at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1445)
        at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1509)
        at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:1332)
        at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:376)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:327)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
        at org.apache.spark.scheduler.Task.run(Task.scala:136)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:548)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1504)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:551)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:750)
Caused by: org.apache.hudi.exception.HoodieException: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.execute(BoundedInMemoryExecutor.java:161)
        at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:147)
        ... 31 more
Caused by: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at java.util.concurrent.FutureTask.report(FutureTask.java:122)
        at java.util.concurrent.FutureTask.get(FutureTask.java:192)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.execute(BoundedInMemoryExecutor.java:155)
        ... 32 more
Caused by: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.throwExceptionIfFailed(BoundedInMemoryQueue.java:248)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.readNextRecord(BoundedInMemoryQueue.java:226)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.access$100(BoundedInMemoryQueue.java:52)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue$QueueIterator.hasNext(BoundedInMemoryQueue.java:278)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer.consume(BoundedInMemoryQueueConsumer.java:36)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$2(BoundedInMemoryExecutor.java:135)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        ... 3 more
Caused by: org.apache.hudi.exception.HoodieException: unable to read next record from parquet file 
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:53)
        at org.apache.hudi.common.util.queue.IteratorBasedQueueProducer.produce(IteratorBasedQueueProducer.java:45)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$0(BoundedInMemoryExecutor.java:106)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
        ... 4 more
Caused by: org.apache.parquet.io.ParquetDecodingException: Can not read value at 1 in block 0 in file file:/Users/kazdy/workspace/hudi-clustering/testtable/partitionCol1=2023/e2f39627-252a-416b-a9d4-1e0c21d10df6-0_0-148-113_20230329224554584.parquet
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:254)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:132)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:136)
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:48)
        ... 8 more
Caused by: java.lang.UnsupportedOperationException: org.apache.parquet.avro.AvroConverters$FieldFixedConverter
        at org.apache.parquet.io.api.PrimitiveConverter.addLong(PrimitiveConverter.java:105)
        at org.apache.parquet.column.impl.ColumnReaderBase$2$4.writeValue(ColumnReaderBase.java:325)
        at org.apache.parquet.column.impl.ColumnReaderBase.writeCurrentValueToConverter(ColumnReaderBase.java:440)
        at org.apache.parquet.column.impl.ColumnReaderImpl.writeCurrentValueToConverter(ColumnReaderImpl.java:30)
        at org.apache.parquet.io.RecordReaderImplementation.read(RecordReaderImplementation.java:406)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:229)
        ... 11 more
23/03/29 22:46:11 WARN BlockManager: Putting block rdd_36_0 failed due to exception org.apache.hudi.exception.HoodieUpsertException: Error upserting bucketType UPDATE for partition :0.
23/03/29 22:46:11 WARN BlockManager: Block rdd_36_0 could not be removed as it was not found on disk or in memory
23/03/29 22:46:11 ERROR Executor: Exception in task 0.0 in stage 14.0 (TID 10)
org.apache.hudi.exception.HoodieUpsertException: Error upserting bucketType UPDATE for partition :0
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpsertPartition(BaseSparkCommitActionExecutor.java:329)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.lambda$mapPartitionsAsRDD$a3ab3c4$1(BaseSparkCommitActionExecutor.java:244)
        at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1(JavaRDDLike.scala:102)
        at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1$adapted(JavaRDDLike.scala:102)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2(RDD.scala:907)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2$adapted(RDD.scala:907)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.$anonfun$getOrCompute$1(RDD.scala:378)
        at org.apache.spark.storage.BlockManager.$anonfun$doPutIterator$1(BlockManager.scala:1535)
        at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1445)
        at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1509)
        at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:1332)
        at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:376)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:327)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
        at org.apache.spark.scheduler.Task.run(Task.scala:136)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:548)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1504)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:551)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:750)
Caused by: org.apache.hudi.exception.HoodieException: org.apache.hudi.exception.HoodieException: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:149)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdateInternal(BaseSparkCommitActionExecutor.java:358)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdate(BaseSparkCommitActionExecutor.java:349)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpsertPartition(BaseSparkCommitActionExecutor.java:322)
        ... 28 more
Caused by: org.apache.hudi.exception.HoodieException: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.execute(BoundedInMemoryExecutor.java:161)
        at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:147)
        ... 31 more
Caused by: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at java.util.concurrent.FutureTask.report(FutureTask.java:122)
        at java.util.concurrent.FutureTask.get(FutureTask.java:192)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.execute(BoundedInMemoryExecutor.java:155)
        ... 32 more
Caused by: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.throwExceptionIfFailed(BoundedInMemoryQueue.java:248)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.readNextRecord(BoundedInMemoryQueue.java:226)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.access$100(BoundedInMemoryQueue.java:52)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue$QueueIterator.hasNext(BoundedInMemoryQueue.java:278)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer.consume(BoundedInMemoryQueueConsumer.java:36)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$2(BoundedInMemoryExecutor.java:135)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        ... 3 more
Caused by: org.apache.hudi.exception.HoodieException: unable to read next record from parquet file 
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:53)
        at org.apache.hudi.common.util.queue.IteratorBasedQueueProducer.produce(IteratorBasedQueueProducer.java:45)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$0(BoundedInMemoryExecutor.java:106)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
        ... 4 more
Caused by: org.apache.parquet.io.ParquetDecodingException: Can not read value at 1 in block 0 in file file:/Users/kazdy/workspace/hudi-clustering/testtable/partitionCol1=2023/e2f39627-252a-416b-a9d4-1e0c21d10df6-0_0-148-113_20230329224554584.parquet
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:254)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:132)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:136)
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:48)
        ... 8 more
Caused by: java.lang.UnsupportedOperationException: org.apache.parquet.avro.AvroConverters$FieldFixedConverter
        at org.apache.parquet.io.api.PrimitiveConverter.addLong(PrimitiveConverter.java:105)
        at org.apache.parquet.column.impl.ColumnReaderBase$2$4.writeValue(ColumnReaderBase.java:325)
        at org.apache.parquet.column.impl.ColumnReaderBase.writeCurrentValueToConverter(ColumnReaderBase.java:440)
        at org.apache.parquet.column.impl.ColumnReaderImpl.writeCurrentValueToConverter(ColumnReaderImpl.java:30)
        at org.apache.parquet.io.RecordReaderImplementation.read(RecordReaderImplementation.java:406)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:229)
        ... 11 more
23/03/29 22:46:11 WARN TaskSetManager: Lost task 0.0 in stage 14.0 (TID 10) (192.168.0.184 executor driver): org.apache.hudi.exception.HoodieUpsertException: Error upserting bucketType UPDATE for partition :0
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpsertPartition(BaseSparkCommitActionExecutor.java:329)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.lambda$mapPartitionsAsRDD$a3ab3c4$1(BaseSparkCommitActionExecutor.java:244)
        at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1(JavaRDDLike.scala:102)
        at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1$adapted(JavaRDDLike.scala:102)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2(RDD.scala:907)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2$adapted(RDD.scala:907)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.$anonfun$getOrCompute$1(RDD.scala:378)
        at org.apache.spark.storage.BlockManager.$anonfun$doPutIterator$1(BlockManager.scala:1535)
        at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1445)
        at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1509)
        at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:1332)
        at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:376)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:327)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
        at org.apache.spark.scheduler.Task.run(Task.scala:136)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:548)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1504)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:551)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:750)
Caused by: org.apache.hudi.exception.HoodieException: org.apache.hudi.exception.HoodieException: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:149)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdateInternal(BaseSparkCommitActionExecutor.java:358)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdate(BaseSparkCommitActionExecutor.java:349)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpsertPartition(BaseSparkCommitActionExecutor.java:322)
        ... 28 more
Caused by: org.apache.hudi.exception.HoodieException: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.execute(BoundedInMemoryExecutor.java:161)
        at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:147)
        ... 31 more
Caused by: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at java.util.concurrent.FutureTask.report(FutureTask.java:122)
        at java.util.concurrent.FutureTask.get(FutureTask.java:192)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.execute(BoundedInMemoryExecutor.java:155)
        ... 32 more
Caused by: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.throwExceptionIfFailed(BoundedInMemoryQueue.java:248)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.readNextRecord(BoundedInMemoryQueue.java:226)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.access$100(BoundedInMemoryQueue.java:52)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue$QueueIterator.hasNext(BoundedInMemoryQueue.java:278)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer.consume(BoundedInMemoryQueueConsumer.java:36)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$2(BoundedInMemoryExecutor.java:135)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        ... 3 more
Caused by: org.apache.hudi.exception.HoodieException: unable to read next record from parquet file 
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:53)
        at org.apache.hudi.common.util.queue.IteratorBasedQueueProducer.produce(IteratorBasedQueueProducer.java:45)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$0(BoundedInMemoryExecutor.java:106)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
        ... 4 more
Caused by: org.apache.parquet.io.ParquetDecodingException: Can not read value at 1 in block 0 in file file:/Users/kazdy/workspace/hudi-clustering/testtable/partitionCol1=2023/e2f39627-252a-416b-a9d4-1e0c21d10df6-0_0-148-113_20230329224554584.parquet
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:254)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:132)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:136)
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:48)
        ... 8 more
Caused by: java.lang.UnsupportedOperationException: org.apache.parquet.avro.AvroConverters$FieldFixedConverter
        at org.apache.parquet.io.api.PrimitiveConverter.addLong(PrimitiveConverter.java:105)
        at org.apache.parquet.column.impl.ColumnReaderBase$2$4.writeValue(ColumnReaderBase.java:325)
        at org.apache.parquet.column.impl.ColumnReaderBase.writeCurrentValueToConverter(ColumnReaderBase.java:440)
        at org.apache.parquet.column.impl.ColumnReaderImpl.writeCurrentValueToConverter(ColumnReaderImpl.java:30)
        at org.apache.parquet.io.RecordReaderImplementation.read(RecordReaderImplementation.java:406)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:229)
        ... 11 more

23/03/29 22:46:11 ERROR TaskSetManager: Task 0 in stage 14.0 failed 1 times; aborting job
Traceback (most recent call last):
  File "/Users/kazdy/workspace/hudi-clustering/replicate_2.py", line 36, in <module>
    df.write.format("hudi").mode("append").options(**hudi_options).save(path) # this should throw error, not able to update records in clustered file 
  File "/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/site-packages/pyspark/sql/readwriter.py", line 968, in save
    self._jwrite.save(path)
  File "/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/site-packages/py4j/java_gateway.py", line 1321, in __call__
    return_value = get_return_value(
  File "/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/site-packages/pyspark/sql/utils.py", line 190, in deco
    return f(*a, **kw)
  File "/Library/Frameworks/Python.framework/Versions/3.10/lib/python3.10/site-packages/py4j/protocol.py", line 326, in get_return_value
    raise Py4JJavaError(
py4j.protocol.Py4JJavaError: An error occurred while calling o61.save.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 14.0 failed 1 times, most recent failure: Lost task 0.0 in stage 14.0 (TID 10) (192.168.0.184 executor driver): org.apache.hudi.exception.HoodieUpsertException: Error upserting bucketType UPDATE for partition :0
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpsertPartition(BaseSparkCommitActionExecutor.java:329)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.lambda$mapPartitionsAsRDD$a3ab3c4$1(BaseSparkCommitActionExecutor.java:244)
        at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1(JavaRDDLike.scala:102)
        at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1$adapted(JavaRDDLike.scala:102)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2(RDD.scala:907)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2$adapted(RDD.scala:907)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.$anonfun$getOrCompute$1(RDD.scala:378)
        at org.apache.spark.storage.BlockManager.$anonfun$doPutIterator$1(BlockManager.scala:1535)
        at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1445)
        at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1509)
        at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:1332)
        at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:376)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:327)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
        at org.apache.spark.scheduler.Task.run(Task.scala:136)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:548)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1504)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:551)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:750)
Caused by: org.apache.hudi.exception.HoodieException: org.apache.hudi.exception.HoodieException: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:149)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdateInternal(BaseSparkCommitActionExecutor.java:358)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdate(BaseSparkCommitActionExecutor.java:349)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpsertPartition(BaseSparkCommitActionExecutor.java:322)
        ... 28 more
Caused by: org.apache.hudi.exception.HoodieException: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.execute(BoundedInMemoryExecutor.java:161)
        at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:147)
        ... 31 more
Caused by: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at java.util.concurrent.FutureTask.report(FutureTask.java:122)
        at java.util.concurrent.FutureTask.get(FutureTask.java:192)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.execute(BoundedInMemoryExecutor.java:155)
        ... 32 more
Caused by: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.throwExceptionIfFailed(BoundedInMemoryQueue.java:248)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.readNextRecord(BoundedInMemoryQueue.java:226)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.access$100(BoundedInMemoryQueue.java:52)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue$QueueIterator.hasNext(BoundedInMemoryQueue.java:278)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer.consume(BoundedInMemoryQueueConsumer.java:36)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$2(BoundedInMemoryExecutor.java:135)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        ... 3 more
Caused by: org.apache.hudi.exception.HoodieException: unable to read next record from parquet file 
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:53)
        at org.apache.hudi.common.util.queue.IteratorBasedQueueProducer.produce(IteratorBasedQueueProducer.java:45)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$0(BoundedInMemoryExecutor.java:106)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
        ... 4 more
Caused by: org.apache.parquet.io.ParquetDecodingException: Can not read value at 1 in block 0 in file file:/Users/kazdy/workspace/hudi-clustering/testtable/partitionCol1=2023/e2f39627-252a-416b-a9d4-1e0c21d10df6-0_0-148-113_20230329224554584.parquet
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:254)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:132)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:136)
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:48)
        ... 8 more
Caused by: java.lang.UnsupportedOperationException: org.apache.parquet.avro.AvroConverters$FieldFixedConverter
        at org.apache.parquet.io.api.PrimitiveConverter.addLong(PrimitiveConverter.java:105)
        at org.apache.parquet.column.impl.ColumnReaderBase$2$4.writeValue(ColumnReaderBase.java:325)
        at org.apache.parquet.column.impl.ColumnReaderBase.writeCurrentValueToConverter(ColumnReaderBase.java:440)
        at org.apache.parquet.column.impl.ColumnReaderImpl.writeCurrentValueToConverter(ColumnReaderImpl.java:30)
        at org.apache.parquet.io.RecordReaderImplementation.read(RecordReaderImplementation.java:406)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:229)
        ... 11 more

Driver stacktrace:
        at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2672)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2608)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2607)
        at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
        at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
        at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
        at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2607)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:1182)
        at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:1182)
        at scala.Option.foreach(Option.scala:407)
        at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:1182)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2860)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2802)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2791)
        at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
        at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:952)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2228)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2249)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2268)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:2293)
        at org.apache.spark.rdd.RDD.count(RDD.scala:1274)
        at org.apache.hudi.HoodieSparkSqlWriter$.commitAndPerformPostOperations(HoodieSparkSqlWriter.scala:706)
        at org.apache.hudi.HoodieSparkSqlWriter$.write(HoodieSparkSqlWriter.scala:340)
        at org.apache.hudi.DefaultSource.createRelation(DefaultSource.scala:144)
        at org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand.run(SaveIntoDataSourceCommand.scala:47)
        at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:75)
        at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:73)
        at org.apache.spark.sql.execution.command.ExecutedCommandExec.executeCollect(commands.scala:84)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:98)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:109)
        at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:169)
        at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:95)
        at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:779)
        at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:64)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
        at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:94)
        at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:584)
        at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:176)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:584)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:30)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
        at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:30)
        at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:30)
        at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:560)
        at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:94)
        at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:81)
        at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:79)
        at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:116)
        at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:860)
        at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:390)
        at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:363)
        at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:239)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
        at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
        at py4j.Gateway.invoke(Gateway.java:282)
        at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
        at py4j.commands.CallCommand.execute(CallCommand.java:79)
        at py4j.ClientServerConnection.waitForCommands(ClientServerConnection.java:182)
        at py4j.ClientServerConnection.run(ClientServerConnection.java:106)
        at java.lang.Thread.run(Thread.java:750)
Caused by: org.apache.hudi.exception.HoodieUpsertException: Error upserting bucketType UPDATE for partition :0
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpsertPartition(BaseSparkCommitActionExecutor.java:329)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.lambda$mapPartitionsAsRDD$a3ab3c4$1(BaseSparkCommitActionExecutor.java:244)
        at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1(JavaRDDLike.scala:102)
        at org.apache.spark.api.java.JavaRDDLike.$anonfun$mapPartitionsWithIndex$1$adapted(JavaRDDLike.scala:102)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2(RDD.scala:907)
        at org.apache.spark.rdd.RDD.$anonfun$mapPartitionsWithIndex$2$adapted(RDD.scala:907)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.$anonfun$getOrCompute$1(RDD.scala:378)
        at org.apache.spark.storage.BlockManager.$anonfun$doPutIterator$1(BlockManager.scala:1535)
        at org.apache.spark.storage.BlockManager.org$apache$spark$storage$BlockManager$$doPut(BlockManager.scala:1445)
        at org.apache.spark.storage.BlockManager.doPutIterator(BlockManager.scala:1509)
        at org.apache.spark.storage.BlockManager.getOrElseUpdate(BlockManager.scala:1332)
        at org.apache.spark.rdd.RDD.getOrCompute(RDD.scala:376)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:327)
        at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:365)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:329)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
        at org.apache.spark.scheduler.Task.run(Task.scala:136)
        at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:548)
        at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1504)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:551)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        ... 1 more
Caused by: org.apache.hudi.exception.HoodieException: org.apache.hudi.exception.HoodieException: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:149)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdateInternal(BaseSparkCommitActionExecutor.java:358)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpdate(BaseSparkCommitActionExecutor.java:349)
        at org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor.handleUpsertPartition(BaseSparkCommitActionExecutor.java:322)
        ... 28 more
Caused by: org.apache.hudi.exception.HoodieException: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.execute(BoundedInMemoryExecutor.java:161)
        at org.apache.hudi.table.action.commit.HoodieMergeHelper.runMerge(HoodieMergeHelper.java:147)
        ... 31 more
Caused by: java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: operation has failed
        at java.util.concurrent.FutureTask.report(FutureTask.java:122)
        at java.util.concurrent.FutureTask.get(FutureTask.java:192)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.execute(BoundedInMemoryExecutor.java:155)
        ... 32 more
Caused by: org.apache.hudi.exception.HoodieException: operation has failed
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.throwExceptionIfFailed(BoundedInMemoryQueue.java:248)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.readNextRecord(BoundedInMemoryQueue.java:226)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue.access$100(BoundedInMemoryQueue.java:52)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueue$QueueIterator.hasNext(BoundedInMemoryQueue.java:278)
        at org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer.consume(BoundedInMemoryQueueConsumer.java:36)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$2(BoundedInMemoryExecutor.java:135)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        ... 3 more
Caused by: org.apache.hudi.exception.HoodieException: unable to read next record from parquet file 
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:53)
        at org.apache.hudi.common.util.queue.IteratorBasedQueueProducer.produce(IteratorBasedQueueProducer.java:45)
        at org.apache.hudi.common.util.queue.BoundedInMemoryExecutor.lambda$null$0(BoundedInMemoryExecutor.java:106)
        at java.util.concurrent.FutureTask.run(FutureTask.java:266)
        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
        ... 4 more
Caused by: org.apache.parquet.io.ParquetDecodingException: Can not read value at 1 in block 0 in file file:/Users/kazdy/workspace/hudi-clustering/testtable/partitionCol1=2023/e2f39627-252a-416b-a9d4-1e0c21d10df6-0_0-148-113_20230329224554584.parquet
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:254)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:132)
        at org.apache.parquet.hadoop.ParquetReader.read(ParquetReader.java:136)
        at org.apache.hudi.common.util.ParquetReaderIterator.hasNext(ParquetReaderIterator.java:48)
        ... 8 more
Caused by: java.lang.UnsupportedOperationException: org.apache.parquet.avro.AvroConverters$FieldFixedConverter
        at org.apache.parquet.io.api.PrimitiveConverter.addLong(PrimitiveConverter.java:105)
        at org.apache.parquet.column.impl.ColumnReaderBase$2$4.writeValue(ColumnReaderBase.java:325)
        at org.apache.parquet.column.impl.ColumnReaderBase.writeCurrentValueToConverter(ColumnReaderBase.java:440)
        at org.apache.parquet.column.impl.ColumnReaderImpl.writeCurrentValueToConverter(ColumnReaderImpl.java:30)
        at org.apache.parquet.io.RecordReaderImplementation.read(RecordReaderImplementation.java:406)
        at org.apache.parquet.hadoop.InternalParquetRecordReader.nextKeyValue(InternalParquetRecordReader.java:229)
        ... 11 more
```
