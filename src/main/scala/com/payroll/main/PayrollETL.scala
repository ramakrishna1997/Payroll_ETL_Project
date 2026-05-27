package com.payroll.main
import org.apache.spark.sql.SparkSession
import com.payroll.Config.Config
import com.payroll.ingestion.Ingest
import com.payroll.transformation.Transform
import com.payroll.validation.Validation
import com.payroll.analytics.Analytics
import java.io.File
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

object PayrollETL {

  def setupWinutils(): Unit = {
    if (System.getProperty("os.name").toLowerCase.contains("win")) {
      val hadoopHome = new File("winutils_hadoop_3")
      val binDir = new File(hadoopHome, "bin")
      if (!binDir.exists()) binDir.mkdirs()

      val winutilsPath = new File(binDir, "winutils.exe")
      if (!winutilsPath.exists()) {
        try {
          val url = new URL("https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe")
          val in = url.openStream()
          Files.copy(in, Paths.get(winutilsPath.getAbsolutePath), StandardCopyOption.REPLACE_EXISTING)
          in.close()
        } catch {
          case _: Exception =>
            // Offline fallback: Use hostname.exe as a dummy winutils
            try {
              val sysRoot = System.getenv("SystemRoot")
              val dummyExe = new File(if (sysRoot != null) sysRoot else "C:\\Windows", "System32\\hostname.exe")
              if (dummyExe.exists()) {
                Files.copy(Paths.get(dummyExe.getAbsolutePath), Paths.get(winutilsPath.getAbsolutePath), StandardCopyOption.REPLACE_EXISTING)
              } else {
                winutilsPath.createNewFile()
              }
            } catch {
              case _: Exception => winutilsPath.createNewFile()
            }
        }
      }

      val hadoopDllPath = new File(binDir, "hadoop.dll")
      if (!hadoopDllPath.exists()) {
        try {
          val url = new URL("https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll")
          val in = url.openStream()
          Files.copy(in, Paths.get(hadoopDllPath.getAbsolutePath), StandardCopyOption.REPLACE_EXISTING)
          in.close()
        } catch {
          case _: Exception => // ignore
        }
      }

      System.setProperty("hadoop.home.dir", hadoopHome.getAbsolutePath)

      // Try to manually load hadoop.dll so NativeIO.Windows can find access0
      try {
        if (hadoopDllPath.exists()) {
          System.load(hadoopDllPath.getAbsolutePath)
        }
      } catch {
        case e: Throwable =>
          System.err.println("Warning: Failed to load hadoop.dll: " + e.getMessage)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      setupWinutils()

      val spark = SparkSession.builder()
        .appName("Payroll ETL Project")
        .master("local[*]")
        .getOrCreate()

      spark.sparkContext.setLogLevel("ERROR")

      // =============================
      // INGESTION
      // =============================

      val rawDF =
        Ingest.readCSV(
          spark,
          Config.rawPath
        )

      println("========== RAW DATA ==========")

      rawDF.show()

      // =============================
      // VALIDATION
      // =============================

      Validation.validateData(rawDF)

      // =============================
      // TRANSFORMATION
      // =============================

      val transformedDF =
        Transform.cleanData(rawDF)

      println("========== TRANSFORMED DATA ==========")

      transformedDF.show()

      // =============================
      // SAVE PROCESSED DATA
      // =============================

      transformedDF.write
        .mode("overwrite")
        .option("header", "true")
        .csv(Config.processedPath)

      // =============================
      // ANALYTICS
      // =============================

      val deptSalaryDF =
        Analytics.departmentWiseSalary(transformedDF)

      println("========== DEPARTMENT ANALYTICS ==========")

      deptSalaryDF.show()

      val topSalaryDF =
        Analytics.highestSalary(transformedDF)

      println("========== TOP 10 SALARIES ==========")

      topSalaryDF.show()

      // =============================
      // SAVE ANALYTICS
      // =============================

      deptSalaryDF.write
        .mode("overwrite")
        .option("header", "true")
        .csv(Config.outputPath + "department_salary")

      topSalaryDF.write
        .mode("overwrite")
        .option("header", "true")
        .csv(Config.outputPath + "top_salary")

      spark.stop()
    } catch {
      case e: Throwable =>
        System.err.println("ETL Pipeline failed with exception:")
        e.printStackTrace()
        var cause = e.getCause
        while (cause != null) {
          System.err.println("Caused by:")
          cause.printStackTrace()
          cause = cause.getCause
        }
        throw e
    }
  }
}