package com.reazon.Analytics

import com.reazon.FileAccess.DFAO
import com.reazon.Schemas.SalaryData

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

      SalaryData(highestPay.rating,
        highestPay.companyName,
        highestPay.jobTitle,
        highestPay.salary,
        highestPay.salariesReported,
        highestPay.location,
        highestPay.employmentStatus,
        highestPay.jobRoles)

    }.toDS()
      .as[SalaryData] //    need to convert it to proper DS SalaryData
      .cache()


    ratingHighestPay.show()

  }

}
