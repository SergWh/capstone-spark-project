package utils

import org.apache.spark.sql.{Column, DataFrame}

import scala.language.implicitConversions


class DatasetExtension(dataset: DataFrame) {
  def withColumnIfNotExists(name: String, column: Column): DataFrame =
    if (dataset.columns.contains(name)) dataset.toDF() else dataset.withColumn(name, column)
}

object DatasetExtension {
  implicit def datasetToExtension(dataset: DataFrame): DatasetExtension = new DatasetExtension(dataset)
}
