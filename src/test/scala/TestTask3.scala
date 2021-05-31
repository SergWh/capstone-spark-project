import data.Purchase
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.{HavePropertyMatchResult, HavePropertyMatcher, should}
import utils.TimestampExtension.datasetToExtension

import java.sql.Timestamp
import java.time.LocalDate
import scala.language.postfixOps

class TestTask3 extends BaseTest with AnyFlatSpecLike with should.Matchers with CustomPurchaseMatchers {

  import session.implicits._


  private val date2020_12_04 = Timestamp.valueOf("2020-12-04 10:10:10.0")

  private val filteredByDate = Task3.filterByDate(purchasesAttributionProjection, date2020_12_04)
    .as[Purchase]
    .collect()
    .toList
  private val filteredByMonthYear = Task3.filterByYearAndMonth(purchasesAttributionProjection, date2020_12_04)
    .as[Purchase]
    .collect()
    .toList

  all(filteredByDate) should have(date(date2020_12_04.date))

  all(filteredByMonthYear) should have(month(date2020_12_04.month), year(date2020_12_04.year))


}

trait CustomPurchaseMatchers {

  def year(expected: Int): HavePropertyMatcher[Purchase, Int] = (purchase: Purchase) => HavePropertyMatchResult(
    purchase.purchaseTime.year == expected,
    "year",
    expectedValue = expected,
    actualValue = purchase.purchaseTime.year
  )

  def month(expected: Int): HavePropertyMatcher[Purchase, Int] = (purchase: Purchase) => HavePropertyMatchResult(
    purchase.purchaseTime.month == expected,
    "month",
    expectedValue = expected,
    actualValue = purchase.purchaseTime.month
  )

  def date(expected: LocalDate): HavePropertyMatcher[Purchase, LocalDate] = (purchase: Purchase) => HavePropertyMatchResult(
    purchase.purchaseTime.date.equals(expected),
    "date",
    expectedValue = expected,
    actualValue = purchase.purchaseTime.date
  )

}
