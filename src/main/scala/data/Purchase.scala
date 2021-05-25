package data

import java.sql.Timestamp

case class Purchase(
                     purchaseId: String,
                     channelId: String,
                     campaignId: String,
                     sessionId: String,
                     purchaseTime: Timestamp,
                     billingCost: Option[Double],
                     isConfirmed: Boolean
                   )

