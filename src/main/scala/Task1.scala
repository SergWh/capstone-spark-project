import data.{Attributes, AttributesBuf, ClickStreamPurchase, PurchaseWithAttributes, UserPurchase}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, MapType, StringType}
import org.apache.spark.sql._

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

  /**
   * Additional task
   * Datasets API implementation
   */
  def aggregatePurchasesTyped(df: DataFrame): Dataset[PurchaseWithAttributes] = {
    val selectedFieldsDs = getClickStreamFieldsDf(df).as[ClickStreamPurchase]

    selectedFieldsDs
      .groupByKey(_.userId)
      .agg(attributesAggregator.toColumn, typedPurchaseAggregator.toColumn)
      .flatMap {
        case (_, attrs, purchases) =>
          purchases.map(purchase =>
            PurchaseWithAttributes(
              purchase.purchaseId,
              attrs.channelId,
              attrs.campaignId,
              attrs.sessionStart + ":" + attrs.sessionEnd))
      }
  }

  def userPurchaseTyped(df: DataFrame): Dataset[UserPurchase] = {
    df
      .withColumn("purchaseTime", to_timestamp($"purchaseTime"))
      .withColumn("billingCost", $"billingCost".cast(DoubleType))
      .as[UserPurchase]
  }

  private val typedPurchaseAggregator = new Aggregator[ClickStreamPurchase, Array[ClickStreamPurchase], Array[ClickStreamPurchase]] {
    override def zero: Array[ClickStreamPurchase] = Array.empty

    override def reduce(b: Array[ClickStreamPurchase], a: ClickStreamPurchase): Array[ClickStreamPurchase] =
      if (a.eventType == "purchase") b :+ a else b

    override def merge(b1: Array[ClickStreamPurchase], b2: Array[ClickStreamPurchase]): Array[ClickStreamPurchase] = b1 ++ b2

    override def finish(reduction: Array[ClickStreamPurchase]): Array[ClickStreamPurchase] = reduction

    override def bufferEncoder: Encoder[Array[ClickStreamPurchase]] = implicitly(ExpressionEncoder[Array[ClickStreamPurchase]])

    override def outputEncoder: Encoder[Array[ClickStreamPurchase]] = implicitly(ExpressionEncoder[Array[ClickStreamPurchase]])
  }

  private val attributesAggregator = new Aggregator[ClickStreamPurchase, AttributesBuf, Attributes] {

    override def zero: AttributesBuf = AttributesBuf(None, None, None, None)

    override def reduce(b: AttributesBuf, a: ClickStreamPurchase): AttributesBuf = {
      AttributesBuf(
        b.sessionStart orElse Option(a.sessionStart),
        b.sessionEnd orElse Option(a.sessionEnd),
        b.campaignId orElse Option(a.campaignId),
        b.channelId orElse Option(a.channelId)
      )
    }

    override def merge(b1: AttributesBuf, b2: AttributesBuf): AttributesBuf = {
      AttributesBuf(
        b1.sessionStart orElse b2.sessionStart,
        b1.sessionEnd orElse b2.sessionEnd,
        b1.campaignId orElse b2.campaignId,
        b1.channelId orElse b2.channelId
      )
    }

    override def finish(reduction: AttributesBuf): Attributes = Attributes(
      reduction.sessionStart.getOrElse(""),
      reduction.sessionEnd.getOrElse(""),
      reduction.campaignId.getOrElse(""),
      reduction.channelId.getOrElse("")
    )

    override def bufferEncoder: Encoder[AttributesBuf] = implicitly(ExpressionEncoder[AttributesBuf])

    override def outputEncoder: Encoder[Attributes] = implicitly(ExpressionEncoder[Attributes])
  }
}
