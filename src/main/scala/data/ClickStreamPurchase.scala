package data

case class ClickStreamPurchase(
                                userId: String,
                                eventType: String,
                                purchaseId: String,
                                channelId: String,
                                campaignId: String,
                                sessionStart: String,
                                sessionEnd: String
                              )
