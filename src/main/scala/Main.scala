import org.apache.spark.sql._
import utils.savePlan

import java.sql.Timestamp


object Main {

  val clickStreamFolder = "bigdata-input-generator/capstone-dataset/mobile_app_clickstream/"
  val userPurchasesFolder = "bigdata-input-generator/capstone-dataset/user_purchases/"

  val clickStreamParquet = "parquet-dataset/click_stream"
  val userPurchasesParquet = "parquet-dataset/user_purchases"
  val purchasesAttributionParquet = "parquet-dataset/purchases_attribution"

  val topChannelsParquet = "parquet-dataset/top_channels"
  val topCampaignsParquet = "parquet-dataset/top_campaigns"

  val weeklyPurchasesAttributionParquet = "parquet-dataset/weekly_purchases_attribution"
  val september2020Parquet = "parquet-dataset/sept2020"
  val day2020_11_11Parquet = "parquet-dataset/2020-11-11"

  val plansFolder = "plans_parquet_vs_csv"

  private val september2020 = Timestamp.valueOf("2020-09-01 10:10:10.0")
  private val day2020_11_11 = Timestamp.valueOf("2020-11-11 10:10:10.0")

  implicit val session: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Spark Capstone")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    //Task1
    val clickStreamDf = Loader.readCsvGzFiles(clickStreamFolder)
    val userPurchasesDf = Loader.readCsvGzFiles(userPurchasesFolder)
    val task1 = new Task1
    val purchasesAttributionDfCsv = task1.aggregatePurchasesDf(clickStreamDf)
      .join(userPurchasesDf, "purchaseId")

    Task3.writePurchasesAttributionProjectionDf(purchasesAttributionDfCsv, purchasesAttributionParquet)

    //Task2
    val task2 = new Task2
    val topChannelsOverall = task2.getTopCampaigns(purchasesAttributionDfCsv)
    val topCampaignsOverall = task2.getMostPopularChannels(purchasesAttributionDfCsv)
    Task3.writeParquet(topChannelsOverall, topChannelsParquet)
    Task3.writeParquet(topCampaignsOverall, topCampaignsParquet)

    //Task3
    val csvName = "csv.MD"
    val parquetName = "parquet.MD"

    //Task3.1
    Task3.writeUserPurchasesDf(userPurchasesDf, userPurchasesParquet)
    Task3.writeParquet(clickStreamDf, clickStreamParquet)

    val clickStreamDfParquet = Loader.readParquet(clickStreamParquet)
    val userPurchasesDfParquet = Loader.readParquet(userPurchasesParquet)
    val purchasesAttributionDfParquet = task1.aggregatePurchasesDf(clickStreamDfParquet)
      .join(userPurchasesDfParquet, "purchaseId")

    savePlanTask3_1(purchasesAttributionDfCsv, csvName)
    savePlanTask3_1(purchasesAttributionDfParquet, parquetName)

    //Task3.2
    type SavePlansFun = (DataFrame, Timestamp, String) => Unit
    val savePlansFilteredByDate: SavePlansFun = savePlansTask3_2(Task3.filterByDate, "by_date")
    val savePlansFilteredByYearMonth: SavePlansFun = savePlansTask3_2(Task3.filterByYearAndMonth, "by_year_month")
    savePlansFilteredByDate(purchasesAttributionDfCsv, day2020_11_11, csvName)
    savePlansFilteredByDate(purchasesAttributionDfParquet, day2020_11_11, parquetName)
    savePlansFilteredByYearMonth(purchasesAttributionDfCsv, september2020, csvName)
    savePlansFilteredByYearMonth(purchasesAttributionDfParquet, september2020, parquetName)

    //Task3 final
    Task3.writeByWeek(purchasesAttributionDfCsv, quarterNum = 4, weeklyPurchasesAttributionParquet)

    System.in.read() // to keep Spark UI opened
    session.close()
  }

  private def savePlanTask3_1(purchasesAttributionDf: DataFrame, filename: String): Unit = {
    val path = plansFolder + "/task3_1/"
    savePlan(purchasesAttributionDf, path = path, filename = filename)
  }

  private def savePlansTask3_2(filterByTimestamp: (DataFrame, Timestamp) => DataFrame, subFolder: String)
                              (purchasesAttributionDf: DataFrame, timestamp: Timestamp, filename: String): Unit = {
    val path = plansFolder + "/task3_2/" + subFolder
    val filteredDf = filterByTimestamp(purchasesAttributionDf, timestamp)
    new Task2 {
      savePlan(
        getTopCampaigns(filteredDf),
        path = path,
        filename = "/campaigns_" + filename
      )
      savePlan(
        getMostPopularChannels(filteredDf),
        path = path,
        filename = "/channels_" + filename
      )
    }
  }

}