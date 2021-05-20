import org.apache.spark.sql._


object Main {

  val clickStreamFolder = "bigdata-input-generator/capstone-dataset/mobile_app_clickstream/"
  val userPurchasesFolder = "bigdata-input-generator/capstone-dataset/user_purchases/"

  implicit val session: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Spark Capstone")
    .getOrCreate()


  def main(args: Array[String]): Unit = {
    val clickStreamDf = Loader.readCsvGzFiles(clickStreamFolder)
    val userPurchasesDf = Loader.readCsvGzFiles(userPurchasesFolder)

    val task1 = new Task1

    val withDf = task1.aggregatePurchasesDf(clickStreamDf)
    val withAggregator = task1.aggregatePurchasesWithCustomAggregator(clickStreamDf)

    val purchasesAttributionDf = withDf.join(userPurchasesDf, "purchaseId")
    purchasesAttributionDf.printSchema()
    purchasesAttributionDf.explain()

    val purchasesAttributionDfWithAggregator = withAggregator.join(userPurchasesDf, "purchaseId")
    purchasesAttributionDfWithAggregator.printSchema()
    purchasesAttributionDfWithAggregator.explain()

    val task2 = new Task2

    val topCampaigns = task2.getTopCampaigns(purchasesAttributionDf)
    topCampaigns.explain()
    topCampaigns.printSchema()

    val popularChannels = task2.getMostPopularChannels(purchasesAttributionDf)
    popularChannels.explain()
    popularChannels.printSchema()

    session.close()
  }

}