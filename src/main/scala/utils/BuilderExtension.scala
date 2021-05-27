package utils

import org.apache.spark.sql.SparkSession

import scala.language.implicitConversions

class BuilderExtension(builder: SparkSession.Builder) {
  def optionsFromMap(options: Map[String, String]): SparkSession.Builder = {
    options.foreach { case (key, value) => builder.config(key, value) }
    builder
  }
}

object BuilderExtension {
  implicit def builderToExtension(builder: SparkSession.Builder): BuilderExtension = new BuilderExtension(builder)
}


