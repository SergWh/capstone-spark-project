import org.apache.spark.sql.{DataFrame, SparkSession}

object Loader {

  def readCsvGzFiles(path: String)(implicit session: SparkSession): DataFrame = {
    session.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path + "/*.csv.gz")
  }

  def readParquet(path: String)(implicit session: SparkSession): DataFrame = {
    session
      .read
      .option("inferSchema", "true")
      .parquet(path)
  }

}
