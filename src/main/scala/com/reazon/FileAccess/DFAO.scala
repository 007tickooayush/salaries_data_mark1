package com.reazon.FileAccess

import com.reazon.Schemas.SalaryData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkFiles}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}


class DFAO {

  val spark: SparkSession = SparkSession.builder().appName("SALARY DATASET MARK 1").master("local[*]").getOrCreate()

  val sc: SparkContext = spark.sparkContext
  //  importing the implicits for reference

  import spark.implicits._


  def getFiles: Dataset[SalaryData] = {
    //    to suppress BlockManager Exceptions while execution
    Logger.getLogger("org").setLevel(Level.FATAL)

    sc.addFile("src\\data\\Salary_Dataset_with_Extra_Features.csv")

    //    val rawData =
    spark.read
      .option("header", value = true)
      .schema(Encoders.product[SalaryData].schema)
      .csv(SparkFiles.get("Salary_Dataset_with_Extra_Features.csv"))
      .as[SalaryData]
      .cache()
  }

}
