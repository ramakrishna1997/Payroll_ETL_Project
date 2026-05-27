package com.payroll.transformation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Transform {

  def cleanData(df: DataFrame): DataFrame = {

    val cleanedDF = df.na.drop()
      .dropDuplicates()

    val transformedDF = cleanedDF
      .withColumn("Annual_Salary", col("REGULAR_PAY") * 12)
      .withColumn(
        "Bonus",
        col("REGULAR_PAY") * 0.10
      )

    transformedDF
  }
}