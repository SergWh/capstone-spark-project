import org.apache.spark.sql._
import utils.BuilderExtension.builderToExtension
import utils.Loader
import utils.Utils.{loadMainConfigs, loadSparkConfigs, savePlan}

import java.sql.Timestamp


object Main extends App {

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

  saveTask3_1Plans(csvDf = purchasesAttributionDfCsv, parquetDf = purchasesAttributionDfParquet)

  //Task3.2
  saveTask3_2Plans(csvDf = purchasesAttributionDfCsv, parquetDf = purchasesAttributionDfParquet)

  //Task3 final
  Task3.writeByWeek(purchasesAttributionDfCsv, quarterNum = 4, mainConfigs.weeklyPurchasesAttributionParquet)

  System.in.read() // to keep Spark UI opened
  session.close()


  private def saveTask3_1Plans(csvDf: DataFrame, parquetDf: DataFrame): Unit = {
    val path = mainConfigs.plansFolder + "/task3_1/"
    savePlan(csvDf, path = path, filename = csvName)
    savePlan(parquetDf, path = path, filename = parquetName)
  }

  private def saveTask3_2Plans(csvDf: DataFrame, parquetDf: DataFrame): Unit = {
    val september2020 = Timestamp.valueOf("2020-09-01 10:10:10.0")
    val november11 = Timestamp.valueOf("2020-11-11 10:10:10.0")

    val csvNovember11 = Task3.filterByDate(csvDf, november11)
    val parquetNovember11 = Task3.filterByDate(csvDf, november11)
    val datePath = mainConfigs.plansFolder + "/task3_2/by_date/"

    val csvSeptember2020 = Task3.filterByYearAndMonth(csvDf, september2020)
    val parquetSeptember2020 = Task3.filterByYearAndMonth(csvDf, september2020)
    val yearMonthPath = mainConfigs.plansFolder + "/task3_2/by_year_month/"

    new Task2 {
      savePlan(getTopCampaigns(csvNovember11), datePath, "campaigns_" + csvName)
      savePlan(getTopCampaigns(parquetNovember11), datePath, "campaigns_" + parquetName)
      savePlan(getMostPopularChannels(csvNovember11), datePath, "channels_" + csvName)
      savePlan(getMostPopularChannels(parquetNovember11), datePath, "channels_" + parquetName)

      savePlan(getTopCampaigns(csvSeptember2020), yearMonthPath, "campaigns_" + csvName)
      savePlan(getTopCampaigns(parquetSeptember2020), yearMonthPath, "campaigns_" + parquetName)
      savePlan(getMostPopularChannels(csvSeptember2020), yearMonthPath, "channels_" + csvName)
      savePlan(getMostPopularChannels(parquetSeptember2020), yearMonthPath, "channels_" + parquetName)
    }
  }

}