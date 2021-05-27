import org.apache.spark.sql._
import utils.BuilderExtension.builderToExtension
import utils.Utils.{loadMainConfigs, loadSparkConfigs, savePlan}

import java.sql.Timestamp


object Main extends App {

  private val september2020 = Timestamp.valueOf("2020-09-01 10:10:10.0")
  private val november11 = Timestamp.valueOf("2020-11-11 10:10:10.0")

  val mainConfigs = loadMainConfigs("src/main/resources/main.conf")

  implicit val session: SparkSession = SparkSession.builder()
    .optionsFromMap(loadSparkConfigs("src/main/resources/spark.conf"))
    .getOrCreate()

  //Task1
  val clickStreamDf = Loader.readCsvGzFiles(mainConfigs.clickStreamFolder)
  val userPurchasesDf = Loader.readCsvGzFiles(mainConfigs.userPurchasesFolder)
  val task1 = new Task1
  val purchasesAttributionDfCsv = task1.aggregatePurchasesDf(clickStreamDf)
    .join(userPurchasesDf, "purchaseId")

  Task3.writePurchasesAttributionProjectionDf(purchasesAttributionDfCsv, mainConfigs.purchasesAttributionParquet)

  //Task2
  val task2 = new Task2
  val topChannelsOverall = task2.getTopCampaigns(purchasesAttributionDfCsv)
  val topCampaignsOverall = task2.getMostPopularChannels(purchasesAttributionDfCsv)
  Task3.writeParquet(topChannelsOverall, mainConfigs.topChannelsParquet)
  Task3.writeParquet(topCampaignsOverall, mainConfigs.topCampaignsParquet)

  //Task3
  val csvName = "csv.MD"
  val parquetName = "parquet.MD"

  //Task3.1
  Task3.writeUserPurchasesDf(userPurchasesDf, mainConfigs.userPurchasesParquet)
  Task3.writeParquet(clickStreamDf, mainConfigs.clickStreamParquet)

  val clickStreamDfParquet = Loader.readParquet(mainConfigs.clickStreamParquet)
  val userPurchasesDfParquet = Loader.readParquet(mainConfigs.userPurchasesParquet)
  val purchasesAttributionDfParquet = task1.aggregatePurchasesDf(clickStreamDfParquet)
    .join(userPurchasesDfParquet, "purchaseId")

  savePlanTask3_1(purchasesAttributionDfCsv, csvName)
  savePlanTask3_1(purchasesAttributionDfParquet, parquetName)

  //Task3.2
  type SavePlansFun = (DataFrame, Timestamp, String) => Unit
  val savePlansFilteredByDate: SavePlansFun = savePlansTask3_2(Task3.filterByDate, "by_date")
  val savePlansFilteredByYearMonth: SavePlansFun = savePlansTask3_2(Task3.filterByYearAndMonth, "by_year_month")
  savePlansFilteredByDate(purchasesAttributionDfCsv, november11, csvName)
  savePlansFilteredByDate(purchasesAttributionDfParquet, november11, parquetName)
  savePlansFilteredByYearMonth(purchasesAttributionDfCsv, september2020, csvName)
  savePlansFilteredByYearMonth(purchasesAttributionDfParquet, september2020, parquetName)

  //Task3 final
  Task3.writeByWeek(purchasesAttributionDfCsv, quarterNum = 4, mainConfigs.weeklyPurchasesAttributionParquet)

  System.in.read() // to keep Spark UI opened
  session.close()


  private def savePlanTask3_1(purchasesAttributionDf: DataFrame, filename: String): Unit = {
    val path = mainConfigs.plansFolder + "/task3_1/"
    savePlan(purchasesAttributionDf, path = path, filename = filename)
  }

  private def savePlansTask3_2(filterByTimestamp: (DataFrame, Timestamp) => DataFrame, subFolder: String)
                              (purchasesAttributionDf: DataFrame, timestamp: Timestamp, filename: String): Unit = {
    val path = mainConfigs.plansFolder + "/task3_2/" + subFolder
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