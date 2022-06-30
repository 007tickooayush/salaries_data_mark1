package com.reazon.Analytics

import com.reazon.Schemas.SalaryData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.{Encoders, SparkSession}

object HighestPayDefault {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SALARY DATASET MARK 1").master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    //    to suppress BlockManager Exceptions while execution
    Logger.getLogger("org").setLevel(Level.FATAL)

    //  importing the implicits for reference
    import spark.implicits._

    sc.addFile("src\\data\\Salary_Dataset_with_Extra_Features.csv")
    val rawSalData = spark.read
      .option("header", value = true)
      .schema(Encoders.product[SalaryData].schema)
      .csv(SparkFiles.get("Salary_Dataset_with_Extra_Features.csv"))
      .cache()


    //    rawSalData.show(10)

    //    ranking the order of salaries according to highest ones

    val defaultRank = rawSalData.orderBy(Symbol("salary").desc)
    defaultRank.show()

    spark.stop()
  }
}
