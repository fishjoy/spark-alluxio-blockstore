# Spark-alluxio-blockstore
Apache Spark off heap cache block manager over alluxio.
## Configuration
`spark.externalBlockStore.blockManager=org.apache.spark.storage.AlluxioBlockManager`
`spark.externalBlockStore.url=alluxio://localhost:19998`
## Details
Because the api in Alluxio1.0 is quite different with Tachyon, the old TachyonBlockManager cannot work with the Alluxio1.0.
AlluxioBlockManager can replace the default TachyonBlockManager. 
The detail: https://github.com/chengqiangboy/blog/blob/master/spark/spark-off_heap.md
The way using AlluxioBlockManager is the same as default.
```scala
myRdd.persist(StorageLevel.OFF_HEAP)
myDataFrame.persist(StorageLevel.OFF_HEAP)
```


 
