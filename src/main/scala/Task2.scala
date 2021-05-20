import org.apache.spark.sql.{DataFrame, SparkSession}

class Task2(implicit sparkSession: SparkSession) {

  /**
   * Task 2.1
   * Top Campaigns. Plain SQL
   */
  def getTopCampaigns(df: DataFrame): DataFrame = {
    val viewName = s"campaigns"
    df.createOrReplaceTempView(viewName)

    val query = "SELECT campaignId, sum(billingCost) as billSum " +
      s"FROM $viewName " +
      "WHERE isConfirmed = True " +
      "GROUP BY campaignId " +
      "ORDER BY billSum DESC " +
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

}
