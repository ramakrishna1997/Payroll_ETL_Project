package com.payroll.ingestion
import org.apache.spark.sql.{DataFrame, SparkSession}
object Ingest {
  def readCSV(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }
}
