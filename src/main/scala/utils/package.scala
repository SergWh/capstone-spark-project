import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.ExtendedMode

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

package object utils {

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

}
