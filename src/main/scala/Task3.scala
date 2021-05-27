import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import utils.DatasetExtension.datasetToExtension

import java.sql.Timestamp

object Task3 {

  /**
   * Task 3.1
   * Convert input dataset to parquet. Save output for Task #1 as parquet as well
   */
  def writeUserPurchasesDf(df: DataFrame, path: String): Unit = {
    df
      .withColumn("year", year(col("purchaseTime")))
      .withColumn("month", month(col("purchaseTime")))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month")
      .parquet(path)
  }

  def writeParquet(df: DataFrame, path: String): Unit = {
    df
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }

  def writePurchasesAttributionProjectionDf(df: DataFrame, path: String): Unit = {
    df
      .withColumnIfNotExists("year", year(col("purchaseTime")))
      .withColumnIfNotExists("month", month(col("purchaseTime")))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "month")
      .parquet(path)
  }

  /**
   * Task 3.2
   * Calculate metrics from Task #2 for different time periods
   */
  def filterByYearAndMonth(df: DataFrame, timestamp: Timestamp): DataFrame = {
    if (df.columns.contains("year") && df.columns.contains("month")) {
      df.where(
        month(lit(timestamp)) === df("month") &&
          year(lit(timestamp)) === df("year")
      )
    } else {
      df.where(
        month(lit(timestamp)) === month(df("purchaseTime")) &&
          year(lit(timestamp)) === year(df("purchaseTime"))
      )
    }
  }

  def filterByDate(df: DataFrame, timestamp: Timestamp): DataFrame =
    df
      .where(to_date(df("purchaseTime")) === to_date(lit(timestamp)))

  /**
   * Task 3 (last?)
   * Build Weekly purchases Projection within one quarter
   */
  def writeByWeek(df: DataFrame, quarterNum: Int, path: String): Unit = {
    df
      .withColumnIfNotExists("year", year(df("purchaseTime")))
      .withColumn("week", weekofyear(df("purchaseTime")))
      .where(quarter(df("purchaseTime")) === lit(quarterNum))
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("year", "week")
      .parquet(path)
  }

}
