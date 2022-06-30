package com.reazon.Analytics

import com.reazon.FileAccess.DFAO

object HighestPayRating {
  def main(args: Array[String]): Unit = {
    val dfao = new DFAO()

    val (spark, sc, implicits) = dfao.importSpark

    //    import spark.implicits
    import implicits._

    val rawData = dfao.getSalary

    val ratingGroup = rawData.collect().groupBy(_.rating).toSeq.sortBy(_._1).reverse

    //    ratingGroup foreach println

    //    getting highest salary of each rating
    val ratingHighestPay = ratingGroup.map { d =>
      val highestPay = d._2.sortBy(_.salary).reverse.head
      (d._1,highestPay)
    }.toDS()
      .withColumnRenamed("_1","rating")
      .withColumnRenamed("_2","salaryData")
//    need to convert it to proper DS SalaryData

    ratingHighestPay.show()

  }

}
