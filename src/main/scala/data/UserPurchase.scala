package data

import java.sql.Timestamp


case class UserPurchase(
                         purchaseId: String,
                         purchaseTime: Timestamp,
                         billingCost: Option[Double],
                         isConfirmed: Boolean
                       )
