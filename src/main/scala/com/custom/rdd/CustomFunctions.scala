package com.custom.rdd

import org.apache.spark.rdd.RDD

class CustomFunctions(rdd:RDD[SalesRecord]) {
  def totalSales = rdd.map(_.itemValue).sum

  def discount(discountPercentage:Double) = new DiscountRDD(rdd,discountPercentage)
}

object CustomFunctions {
  implicit def addCustomFunctions(rdd: RDD[SalesRecord]) = new
      CustomFunctions(rdd)
}
