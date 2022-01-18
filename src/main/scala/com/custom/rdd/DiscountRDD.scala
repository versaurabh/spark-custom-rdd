package com.custom.rdd

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD

class DiscountRDD(prev:RDD[SalesRecord], discountPercentage:Double)
  extends RDD[SalesRecord](prev) {
  override def compute(split: Partition, context: TaskContext): Iterator[SalesRecord] = {
    firstParent[SalesRecord].iterator(split, context).map(salesRecord => {
      val discounted = salesRecord.itemValue * (1 - discountPercentage)
      new SalesRecord(salesRecord.transactionId, salesRecord.customerId, salesRecord.itemId, discounted)
    })
  }

  override protected def getPartitions: Array[Partition] = prev.partitions
}
