package com.custom.rdd

import java.io.Serializable

class SalesRecord(val transactionId: String,
    val customerId: String,
    val itemId: String,
    val itemValue: Double) extends Comparable[SalesRecord]
  with Serializable {
  override def compareTo(o: SalesRecord): Int = -1
}

