package com.payroll.validation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Validation {

  def validateData(df: DataFrame): Unit = {

    println("========== NULL COUNT ==========")

    df.columns.foreach { column =>

      val nullCount = df.filter(col(column).isNull).count()

      println(s"$column -> $nullCount")
    }

    println("========== DUPLICATE COUNT ==========")

    val duplicateCount =
      df.count() - df.dropDuplicates().count()

    println(s"Duplicate Rows: $duplicateCount")
  }
}