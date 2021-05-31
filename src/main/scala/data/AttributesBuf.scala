package data

case class AttributesBuf(
                          sessionStart: Option[String],
                          sessionEnd: Option[String],
                          campaignId: Option[String],
                          channelId: Option[String]
                        )
