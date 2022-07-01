package com.reazon.Analytics

import com.reazon.FileAccess.DFAO
import com.reazon.Schemas.SalaryData

object HighestPayLocation {
  def main(args: Array[String]): Unit = {
    val dfao = new DFAO()

    val (spark, sc, implicits) = dfao.importSpark

    //    import sql implicit functions
    import implicits._

    val salData = dfao.getSalary

    val locationGroup = salData.collect().groupBy(_.location).toSeq

    val locationHighestPay = locationGroup.map { d =>
      val locHighest = d._2.sortBy(_.salary).reverse.head

      SalaryData(locHighest.rating,
        locHighest.companyName,
        locHighest.jobTitle,
        locHighest.salary,
        locHighest.salariesReported,
        locHighest.location,
        locHighest.employmentStatus,
        locHighest.jobRoles)

    }.toDS()
      .as[SalaryData]
      .orderBy(Symbol("salary").desc) // sorting the resultant dataset according to highest salary
      .cache()

    locationHighestPay.show()

  }

}
