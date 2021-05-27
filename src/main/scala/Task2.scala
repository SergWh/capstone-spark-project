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
      .mapGroups { case (campaignId, purchases) =>
        CampaignBillingCost(
          campaignId,
          purchases.map(_.billingCost.getOrElse(0.0)).sum
        )
      }
      .orderBy($"totalCost".desc)
      .limit(10)

  /**
   * Task 2.2
   * Channels engagement performance. Dataset API
   */
  def getMostPopularChannelsTyped(df: DataFrame): Dataset[ChannelCount] =
    purchasesDataframeToDataset(df)
      .groupByKey(_.channelId)
      .mapGroups { case (channelId, purchases) => ChannelCount(channelId, purchases.size) }
      .orderBy($"sessionCount".desc)

  private def purchasesDataframeToDataset(df: DataFrame) =
    df
      .withColumn("purchaseTime", to_timestamp($"purchaseTime"))
      .withColumn("billingCost", $"billingCost".cast(DoubleType))
      .as[Purchase]

}
