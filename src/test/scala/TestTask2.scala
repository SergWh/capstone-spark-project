import data.{CampaignBillingCost, ChannelCount, Purchase}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should

class TestTask2 extends BaseTest with AnyFlatSpecLike with should.Matchers {

  import session.implicits._

  private val purchasesAttributionArray = purchasesAttributionProjection
    .as[Purchase]
    .collect()

  private val topCampaigns = purchasesAttributionArray
    .groupBy(_.campaignId)
    .map {
      case (campaignId, purchases) => CampaignBillingCost(
        campaignId,
        purchases.foldLeft(0.0)((sum, p) => sum + (if (p.isConfirmed) p.billingCost.getOrElse(0.0) else 0.0))
      )
    }
    .toList
    .sortBy(-_.totalCost)
    .take(10)

  private val popularChannels = purchasesAttributionArray
    .groupBy(_.channelId)
    .map {
      case (channelId, purchases) => ChannelCount(channelId, purchases.length)
    }

  new Task2 {
    getTopCampaigns(purchasesAttributionProjection)
      .as[CampaignBillingCost]
      .collect() should contain theSameElementsAs topCampaigns

    getMostPopularChannels(purchasesAttributionProjection)
      .as[ChannelCount]
      .collect() should contain theSameElementsAs popularChannels

    getTopCampaignsTyped(purchasesAttributionProjection)
      .collect() should contain theSameElementsAs topCampaigns

    getMostPopularChannelsTyped(purchasesAttributionProjection)
      .collect() should contain theSameElementsAs popularChannels
  }

}