package com.reazon

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

object HighestPay {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SALARY DATASET MARK 1").master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    //    to suppress BlockManager Exceptions while execution
    Logger.getLogger("org").setLevel(Level.FATAL)

    //  importing the implicits for reference
    import spark.implicits._

    sc.addFile("src\\data\\Salary_Dataset_with_Extra_Features.csv")
    val rawSalData = spark.read.csv(SparkFiles.get("Salary_Dataset_with_Extra_Features.csv"))

    rawSalData.show(10)


    spark.stop()
  }
}
