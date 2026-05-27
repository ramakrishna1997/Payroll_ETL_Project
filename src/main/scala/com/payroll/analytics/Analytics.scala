package com.payroll.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Analytics {

  def departmentWiseSalary(df: DataFrame): DataFrame = {

    df.groupBy("DEPARTMENT_TITLE")
      .agg(
        avg("Annual_Salary").alias("Average_Salary"),
        sum("Annual_Salary").alias("Total_Salary")
      )
  }

  def highestSalary(df: DataFrame): DataFrame = {

    df.orderBy(col("Annual_Salary").desc)
      .limit(10)
  }
}