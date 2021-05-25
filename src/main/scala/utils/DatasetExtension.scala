package utils

import org.apache.spark.sql.{Column, DataFrame, Dataset}

import scala.language.implicitConversions


class DatasetExtension(dataset: Dataset[_]) {
  def withColumnIfNotExists(name: String, column: Column): DataFrame =
    if (dataset.columns.contains(name)) dataset.toDF() else dataset.withColumn(name, column)
}

object DatasetExtension {
  implicit def datasetToExtension(dataset: Dataset[_]): DatasetExtension = new DatasetExtension(dataset)
}
