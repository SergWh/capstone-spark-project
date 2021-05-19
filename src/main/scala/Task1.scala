import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions.{concat, element_at, first, from_json, lit, when, explode}
import org.apache.spark.sql.types.{MapType, StringType}

class Task1(implicit sparkSession: SparkSession) {

  import sparkSession.implicits._

  /**
   * Task 1.1
   * Implement it by utilizing default Spark SQL capabilities
   */

  def aggregatePurchasesDf(df: DataFrame): DataFrame = {
    val selectedFieldsDf = getClickStreamFieldsDf(df)
    val win = Window.partitionBy($"userId")
    selectedFieldsDf.select(
      $"purchaseId",
      first($"sessionStart", ignoreNulls = true).over(win).as("sessionStart"),
      first($"sessionEnd", ignoreNulls = true).over(win).as("sessionEnd"),
      first($"channelId", ignoreNulls = true).over(win).as("channelId"),
      first($"campaignId", ignoreNulls = true).over(win).as("campaignId"),
    )
      .where($"eventType" === "purchase")
      .withColumn("sessionId", concat($"sessionStart", lit(":"), $"sessionEnd"))
      .drop($"sessionStart")
      .drop($"sessionEnd")
  }

  /**
   * Task 1.2
   * Implement it by using a custom Aggregator or UDAF.
   */

  def aggregatePurchasesWithCustomAggregator(df: DataFrame): DataFrame = {
    val selectedFieldsDf = getClickStreamFieldsDf(df)
    selectedFieldsDf.groupBy($"userId")
      .agg(
        purchasesAggregator.as("purchases"),
        first($"sessionStart", ignoreNulls = true).as("sessionStart"),
        first($"sessionEnd", ignoreNulls = true).as("sessionEnd"),
        first($"channelId", ignoreNulls = true).as("channelId"),
        first($"campaignId", ignoreNulls = true).as("campaignId"),
      )
      .drop($"userId")
      .withColumn("sessionId", concat($"sessionStart", lit(":"), $"sessionEnd"))
      .drop($"sessionStart")
      .drop($"sessionEnd")
      .withColumn("purchaseId", explode($"purchases"))
      .drop($"purchases")
  }

  private val purchasesAggregator = new Aggregator[Row, Array[String], Array[String]] {
    override def zero: Array[String] = Array.empty

    override def reduce(b: Array[String], a: Row): Array[String] = {
      val id = Option(a.getAs[String]("purchaseId"))
      id match {
        case Some(value) => b :+ value
        case None => b
      }
    }

    override def merge(b1: Array[String], b2: Array[String]): Array[String] = b1 ++ b2

    override def finish(reduction: Array[String]): Array[String] = reduction

    override def bufferEncoder: Encoder[Array[String]] = implicitly(ExpressionEncoder[Array[String]])

    override def outputEncoder: Encoder[Array[String]] = implicitly(ExpressionEncoder[Array[String]])
  }.toColumn


  private def getClickStreamFieldsDf(clickStreamDf: DataFrame): DataFrame = {

    val channelId = element_at($"attrs", "channel_id").as("channelId")
    val campaignId = element_at($"attrs", "campaign_id").as("campaignId")
    val purchaseId = element_at($"attrs", "purchase_id").as("purchaseId")

    def makeMarkColumn(eventType: String) =
      when(clickStreamDf("eventType") === eventType, clickStreamDf("eventId"))
        .otherwise(null)

    val sessionStart = makeMarkColumn("app_open").as("sessionStart")
    val sessionEnd = makeMarkColumn("app_close").as("sessionEnd")

    clickStreamDf
      .withColumn("attrs", from_json($"attributes", MapType(StringType, StringType)))
      .select($"userId", $"eventType", purchaseId, channelId, campaignId, sessionStart, sessionEnd)
  }

}
