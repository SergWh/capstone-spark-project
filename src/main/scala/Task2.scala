import data.{CampaignBillingCost, ChannelCount, Purchase}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{count, sum, to_timestamp}
import org.apache.spark.sql.types.DoubleType


class Task2(implicit sparkSession: SparkSession) {

  import sparkSession.implicits._

  /**
   * Task 2.1
   * Top Campaigns. Plain SQL
   */
  def getTopCampaigns(df: DataFrame): DataFrame = {
    val viewName = s"campaigns"
    df.createOrReplaceTempView(viewName)

    val query = "SELECT campaignId, sum(billingCost) as totalCost " +
      s"FROM $viewName " +
      "WHERE isConfirmed = True " +
      "GROUP BY campaignId " +
      "ORDER BY totalCost DESC " +
      "LIMIT 10"

    sparkSession.sql(query)
  }

  /**
   * Task 2.2
   * Channels engagement performance. Plain SQL
   */
  def getMostPopularChannels(df: DataFrame): DataFrame = {
    val viewName = s"channels"
    df.createOrReplaceTempView(viewName)

    val query = "SELECT channelId, count(channelId) as sessionsCount " +
      s"FROM $viewName " +
      "GROUP BY channelId " +
      "ORDER BY sessionsCount DESC "
    sparkSession.sql(query)
  }


  /**
   * Task 2.1
   * Top Campaigns. Dataset API
   */
  def getTopCampaignsTyped(df: DataFrame): Dataset[CampaignBillingCost] =
    purchasesDataframeToDataset(df)
      .groupByKey(_.campaignId)
      .agg(sum($"billingCost").as[Double].name("totalCost"))
      .withColumnRenamed("key", "campaignId")
      .orderBy($"totalCost".desc)
      .limit(10)
      .as[CampaignBillingCost]

  /**
   * Task 2.2
   * Channels engagement performance. Dataset API
   */
  def getMostPopularChannelsTyped(df: DataFrame): Dataset[ChannelCount] =
    purchasesDataframeToDataset(df)
      .groupByKey(_.channelId)
      .agg(count($"channelId").as[Long].name("sessionCount"))
      .withColumnRenamed("key", "channelId")
      .orderBy($"sessionCount".desc)
      .as[ChannelCount]

  private def purchasesDataframeToDataset(df: DataFrame) =
    df
      .withColumn("purchaseTime", to_timestamp($"purchaseTime"))
      .withColumn("billingCost", $"billingCost".cast(DoubleType))
      .as[Purchase]

}
