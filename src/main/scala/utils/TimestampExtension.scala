package utils

import java.sql.Timestamp
import java.time.LocalDate
import java.util.Calendar
import scala.language.implicitConversions

class TimestampExtension(timestamp: Timestamp) {

  private val calendar = Calendar.getInstance()
  calendar.setTimeInMillis(timestamp.getTime)

  val date: LocalDate = timestamp.toLocalDateTime.toLocalDate

  val year: Int = calendar.get(Calendar.YEAR)

  val month: Int = calendar.get(Calendar.MONTH)

}

object TimestampExtension {
  implicit def datasetToExtension(timestamp: Timestamp): TimestampExtension = new TimestampExtension(timestamp)
}
