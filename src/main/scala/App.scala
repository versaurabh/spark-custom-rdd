import com.custom.rdd.SalesRecord
import com.custom.rdd.CustomFunctions._

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    val dataRDD = spark.sparkContext.textFile("src/main/resources/sales.csv")

    val salesRecordRDD = dataRDD.map(row => {
      val colValues = row.split(",")
      new SalesRecord(colValues(0),colValues(1),
        colValues(2),colValues(3).toDouble)
    })

    // custom operator `totalSales` implemented
    // via implicit conversion in `CustomFunctions`
    println(s"Original total: ${salesRecordRDD.totalSales}")

    // custom RDD `DiscountRDD`
    val discountRDD = salesRecordRDD.discount(0.3)
    println(s"Total after discount: ${discountRDD.totalSales}")
  }
}
