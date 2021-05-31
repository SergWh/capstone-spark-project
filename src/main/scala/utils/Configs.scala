package utils

case class MainConfigs(
                        clickStreamFolder: String,
                        userPurchasesFolder: String,
                        clickStreamParquet: String,
                        userPurchasesParquet: String,
                        purchasesAttributionParquet: String,
                        topChannelsParquet: String,
                        topCampaignsParquet: String,
                        weeklyPurchasesAttributionParquet: String,
                        september2020Parquet: String,
                        november11Parquet: String,
                        plansFolder: String
                      )

case class TestConfigs(
                        clickStream: String,
                        userPurchases: String,
                        purchaseAttributionProjection: String
                      )
