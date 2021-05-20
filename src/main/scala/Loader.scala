import org.apache.spark.sql.{DataFrame, SparkSession}

object Loader {

  def readCsvGzFiles(path: String)(implicit session: SparkSession): DataFrame = {
    session.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path + "/*.csv.gz")
  }

}
