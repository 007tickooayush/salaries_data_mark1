package com.reazon.Analytics

import com.reazon.FileAccess.DFAO
import com.reazon.Schemas.SalaryData

object HighestPayJobRole {
  def main(args: Array[String]): Unit = {
    val dfao = new DFAO()

    val (spark, sc, implicits) = dfao.importSpark

    //    import sql implicit functions
    import implicits._

    val data = dfao.getSalary

    val jobRoleGroup = data.collect().groupBy(_.jobRoles).toSeq

    val jobRoleHighestPay = jobRoleGroup.map { d =>
      val pay = d._2.sortBy(_.salary).reverse.head

      SalaryData(pay.rating,
        pay.companyName,
        pay.jobTitle,
        pay.salary,
        pay.salariesReported,
        pay.location,
        pay.employmentStatus,
        pay.jobRoles)

    }.toDS()
      .as[SalaryData]
      .orderBy($"salary".desc)
      .cache()


    jobRoleHighestPay.show()

  }

}
