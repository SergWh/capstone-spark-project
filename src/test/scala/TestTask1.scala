import data.{Purchase, PurchaseWithAttributes}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should

class TestTask1 extends BaseTest with AnyFlatSpecLike with should.Matchers {

  import session.implicits._

  private val purchasesWithAttributes = purchasesAttributionProjection
    .as[Purchase]
    .collect()
    .map(
      purchase => PurchaseWithAttributes(
        purchase.purchaseId,
        purchase.channelId,
        purchase.campaignId,
        purchase.sessionId
      )
    )

  new Task1 {
    aggregatePurchasesDf(clickStreamDf)
      .as[PurchaseWithAttributes]
      .collect() should contain theSameElementsAs purchasesWithAttributes

    aggregatePurchasesTyped(clickStreamDf)
      .collect() should contain theSameElementsAs purchasesWithAttributes

    aggregatePurchasesWithCustomAggregator(clickStreamDf)
      .as[PurchaseWithAttributes]
      .collect() should contain theSameElementsAs purchasesWithAttributes
  }

}
