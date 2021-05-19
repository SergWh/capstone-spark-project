import org.apache.spark.sql.{DataFrame, SparkSession}

class Task2(implicit sparkSession: SparkSession) {

  def getTopCampaigns(df: DataFrame): DataFrame = {
    val viewName = s"campaigns"
    df.createOrReplaceTempView(viewName)

    val query = "SELECT campaignId, sum(billingCost) as billSum " +
      s"FROM $viewName " +
      "WHERE isConfirmed = True " +
      "GROUP BY campaignId " +
      "ORDER BY billSum " +
      "LIMIT 10"

    sparkSession.sql(query)
  }

  def getMostPopularChannels(df: DataFrame): DataFrame = {
    val viewName = s"channels"
    df.createOrReplaceTempView(viewName)

    val query = "SELECT channelId, count(channelId) as sessionsCount " +
      s"FROM $viewName " +
      "GROUP BY channelId " +
      "ORDER BY sessionsCount "
    sparkSession.sql(query)
  }

}
