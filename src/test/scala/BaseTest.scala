import data.Purchase
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import utils.BuilderExtension.builderToExtension
import utils.Utils.{loadSparkConfigs, loadTestConfigs}
import utils.{Loader, TestConfigs}

trait BaseTest {

  implicit val session: SparkSession = SparkSession.builder()
    .optionsFromMap(loadSparkConfigs("src/test/resources/spark.conf"))
    .getOrCreate()

  import session.implicits._

  val testConfigs: TestConfigs = loadTestConfigs("src/test/resources/test.conf")

  val clickStreamDf: DataFrame = Loader.readSingleCsv(testConfigs.clickStream).persist(StorageLevel.MEMORY_ONLY)
  val userPurchasesDf: DataFrame = Loader.readSingleCsv(testConfigs.userPurchases).persist(StorageLevel.MEMORY_ONLY)
  val purchasesAttributionProjection: DataFrame = Loader.readSingleCsv(testConfigs.purchaseAttributionProjection)
    .withColumn("purchaseTime", to_timestamp($"purchaseTime"))
    .withColumn("billingCost", $"billingCost".cast(DoubleType))
    .persist(StorageLevel.MEMORY_ONLY)


}
