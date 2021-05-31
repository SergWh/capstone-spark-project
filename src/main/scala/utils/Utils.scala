package utils

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.ExtendedMode
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import java.io.{InputStream, PrintWriter}
import java.nio.file.{Files, Paths}

object Utils {

  def writeToFile(content: String, path: String, filename: String): Unit = {
    Files.createDirectories(Paths.get(path))
    new PrintWriter(path + filename) {
      write(content)
      close()
    }
  }

  def savePlan(dataset: Dataset[_], path: String, filename: String): Unit = {
    writeToFile(dataset.queryExecution.explainString(ExtendedMode), path, filename)
  }

  def getResourceStream(filename: String): InputStream = getClass.getClassLoader.getResourceAsStream(filename)

  def loadSparkConfigs(filename: String): Map[String, String] = ConfigSource
    .file(filename)
    .load[Map[String, String]]
    .getOrElse(throw new IllegalArgumentException("can't load spark configs"))

  def loadMainConfigs(filename: String): MainConfigs =
    ConfigSource
      .file(filename)
      .load[MainConfigs]
      .getOrElse(throw new IllegalArgumentException("can't load main configs"))

  def loadTestConfigs(filename: String): TestConfigs =
    ConfigSource
      .file(filename)
      .load[TestConfigs]
      .getOrElse(throw new IllegalArgumentException("can't load configs"))

}
