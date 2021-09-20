package com.instagram

import com.instagram.AddressHistoryBuilder.addressHistoryBuilder
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate
import java.time.format.DateTimeFormatter

case class AddressHistory
(id: Long, first_name: String, last_name: String, address: String, moved_in: LocalDate, moved_out: LocalDate, current: Boolean)

case class addressUpdates(id: Long, first_name: String, last_name: String, address: String, moved_in: LocalDate)


class AddressHistoryBuilderSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("History builer App")
    .getOrCreate()

  import spark.implicits._

  val pattern = DateTimeFormatter.ofPattern("dd-MM-yyyy")

  val addressHistorySameAddressForNewRecordUpdate = Seq(
    AddressHistory(4, "houssem", "Dalhoumi", "zalfén", LocalDate.parse("21-11-1960", pattern), null, true)
  ).toDF()
  val newRecordUpdateForSameAdress = Seq(
    addressUpdates(4, "houssem", "Dalhoumi", "zalfén", LocalDate.parse("21-11-1992", pattern))).toDF()
  val expectedResultOfNewRecordAddedOnSameAddress = Seq(
    AddressHistory(4, "houssem", "Dalhoumi", "zalfén", LocalDate.parse("21-11-1960", pattern), null, true)).toDF()


  val addressHistoryDifferentAddressForOldRecordUpdate = Seq(
    AddressHistory(2, "jasser", "rtibi", "amsterdam", LocalDate.parse("21-11-2020", pattern), null, true)).toDF()
  val oldRecordUpdateForDifferentAdress = Seq(
    addressUpdates(2, "jasser", "rtibi", "USA", LocalDate.parse("21-11-1992", pattern))).toDF()
  val expectedResultOfOldRecordAddedOnDifferentAddress = Seq(
    AddressHistory(2, "jasser", "rtibi", "amsterdam", LocalDate.parse("21-11-2020", pattern), null, true),
    AddressHistory(2, "jasser", "rtibi", "USA", LocalDate.parse("21-11-1992", pattern), LocalDate.parse("21-11-2020", pattern), false)
  ).toDF()


  val adressHistoryDifferentAddressForNewRecordUpdate = Seq(
    AddressHistory(1, "Sayf", "Bouazizi", "Kasserine", LocalDate.parse("21-11-1992", pattern), null, true),
    AddressHistory(1, "Sayf", "Bouazizi", "Kasserine", LocalDate.parse("21-11-1960", pattern), LocalDate.parse("21-11-1961", pattern), false)).toDF()
  val newRecordUpdateForDifferentAdress = Seq(
    addressUpdates(1, "Sayf", "Bouazizi", "Sousse", LocalDate.parse("06-06-2017", pattern))).toDF()
  val expectedResultOfNewRecordAddedOnDifferentAddress = Seq(
    AddressHistory(1, "Sayf", "Bouazizi", "Sousse", LocalDate.parse("06-06-2017", pattern), null, true),
    AddressHistory(1, "Sayf", "Bouazizi", "Kasserine", LocalDate.parse("21-11-1992", pattern), LocalDate.parse("06-06-2017", pattern), false),
    AddressHistory(1, "Sayf", "Bouazizi", "Kasserine", LocalDate.parse("21-11-1960", pattern), LocalDate.parse("21-11-1961", pattern), false)).toDF()


  val addressHistorySameAddressForOldRecordUpdate = Seq(
    AddressHistory(3, "oussama", "banana", "Frigya", LocalDate.parse("21-11-2015", pattern), null, true)).toDF()
  val oldRecordUpdateForSameAdress = Seq(
    addressUpdates(3, "oussama", "banana", "Frigya", LocalDate.parse("21-11-2013", pattern))).toDF()
  val expectedResultOfOldRecordAddedOnSameAddress = Seq(
    AddressHistory(3, "oussama", "banana", "Frigya", LocalDate.parse("21-11-2013", pattern), null, true)).toDF()


  val historyThatDoesNotHaveParticularIndividuals = Seq(AddressHistory(4, "houssem", "Dalhoumi", "zalfén", LocalDate.parse("21-11-1960", pattern), null, true)).toDF()
  val individualsThatDoNotHaveHistory = Seq(addressUpdates(5, "badr", "Dalhoumi", "maroc", LocalDate.parse("21-11-1978", pattern))).toDF()
  val expectedResultOfNewOnHistory = Seq(
    AddressHistory(4, "houssem", "Dalhoumi", "zalfén", LocalDate.parse("21-11-1960", pattern), null, true),
    AddressHistory(5, "badr", "Dalhoumi", "maroc", LocalDate.parse("21-11-1978", pattern), null, true)
  ).toDF()


  val adressHistoryOfLeftHouses = Seq(
    AddressHistory(1, "Sayf", "Bouazizi", "Kasserine", LocalDate.parse("21-11-2020", pattern), null, true),
    AddressHistory(1, "Sayf", "Bouazizi", "France", LocalDate.parse("21-11-2019", pattern), LocalDate.parse("21-11-2020", pattern), false),
    AddressHistory(1, "Sayf", "Bouazizi", "Frigya", LocalDate.parse("21-11-2017", pattern), LocalDate.parse("21-11-2019", pattern), false),
    AddressHistory(1, "Sayf", "Bouazizi", "amsterdam", LocalDate.parse("21-11-2015", pattern), LocalDate.parse("21-11-2017", pattern), false)
  ).toDF()
  val historyUpdatesThatOverlapsWithoutOldHistory = Seq(
    addressUpdates(1, "Sayf", "Bouazizi", "Sousse", LocalDate.parse("21-11-2018", pattern))
  ).toDF()
  val expectedResultOfOverlap = Seq(
    AddressHistory(1, "Sayf", "Bouazizi", "Sousse", LocalDate.parse("21-11-2018", pattern), LocalDate.parse("21-11-2019", pattern), false),
    AddressHistory(1, "Sayf", "Bouazizi", "Kasserine", LocalDate.parse("21-11-2020", pattern), null, true),
    AddressHistory(1, "Sayf", "Bouazizi", "France", LocalDate.parse("21-11-2019", pattern), LocalDate.parse("21-11-2020", pattern), false),
    AddressHistory(1, "Sayf", "Bouazizi", "amsterdam", LocalDate.parse("21-11-2015", pattern), LocalDate.parse("21-11-2017", pattern), false),
    AddressHistory(1, "Sayf", "Bouazizi", "Frigya", LocalDate.parse("21-11-2017", pattern), LocalDate.parse("21-11-2018", pattern), false)
  ).toDF()

  val addreshistory = Seq(
    AddressHistory(1, "Sayf", "Bouazizi", "Kasserine", LocalDate.parse("21-11-2020", pattern), null, true),
    AddressHistory(1, "Sayf", "Bouazizi", "amsterdam", LocalDate.parse("21-11-2015", pattern), LocalDate.parse("21-11-2019", pattern), false),
    AddressHistory(1, "Sayf", "Bouazizi", "France", LocalDate.parse("21-11-2019", pattern), LocalDate.parse("21-11-2020", pattern), false)
  ).toDF()
  val historyUpdatethatisTheFurthestInThePast = Seq(
    addressUpdates(1, "Sayf", "Bouazizi", "Sousse", LocalDate.parse("21-11-1960", pattern))
  ).toDF()
  val oldestCaseExpectedResult = Seq(
    AddressHistory(1, "Sayf", "Bouazizi", "Kasserine", LocalDate.parse("21-11-2020", pattern), null, true),
    AddressHistory(1, "Sayf", "Bouazizi", "France", LocalDate.parse("21-11-2019", pattern), LocalDate.parse("21-11-2020", pattern), false),
    AddressHistory(1, "Sayf", "Bouazizi", "amsterdam", LocalDate.parse("21-11-2015", pattern), LocalDate.parse("21-11-2019", pattern), false),
    AddressHistory(1, "Sayf", "Bouazizi", "Sousse", LocalDate.parse("21-11-1960", pattern), LocalDate.parse("21-11-2015", pattern), false)
  ).toDF()
  "AddressHistoryBuilder" should "update the address history when given an update" in {
    Given("the address history and the update")
    val History = addressHistorySameAddressForNewRecordUpdate
    val update = newRecordUpdateForSameAdress
    val History1 = addressHistoryDifferentAddressForOldRecordUpdate
    val update1 = oldRecordUpdateForDifferentAdress
    val history2 = adressHistoryDifferentAddressForNewRecordUpdate
    val update2 = newRecordUpdateForDifferentAdress
    val history3 = addressHistorySameAddressForOldRecordUpdate
    val update3 = oldRecordUpdateForSameAdress
    val history4 = historyThatDoesNotHaveParticularIndividuals
    val update4 = individualsThatDoNotHaveHistory
    val history5 = adressHistoryOfLeftHouses
    val update5 = historyUpdatesThatOverlapsWithoutOldHistory
    val history6 = addreshistory
    val update6 = historyUpdatethatisTheFurthestInThePast

    When("AddressHistoryBuilder is Invoked")
    val result = addressHistoryBuilder(History, update)
    val result1 = addressHistoryBuilder(History1, update1)
    val result2 = addressHistoryBuilder(history2, update2)
    val result3 = addressHistoryBuilder(history3, update3)
    val result4 = addressHistoryBuilder(history4, update4)
    val result5 = addressHistoryBuilder(history5, update5)
    val result6 = addressHistoryBuilder(history6, update6)

    Then("the address history should be updated")
    expectedResultOfNewRecordAddedOnSameAddress.collect() should contain theSameElementsAs (result.collect())
    expectedResultOfOldRecordAddedOnDifferentAddress.collect() should contain theSameElementsAs (result1.collect())
    expectedResultOfNewRecordAddedOnDifferentAddress.collect() should contain theSameElementsAs (result2.collect())
    expectedResultOfOldRecordAddedOnSameAddress.collect() should contain theSameElementsAs (result3.collect())
    expectedResultOfNewOnHistory.collect() should contain theSameElementsAs (result4.collect())
    expectedResultOfOverlap.collect() should contain theSameElementsAs (result5.collect())
    oldestCaseExpectedResult.collect() should contain theSameElementsAs(result6.collect())
  }
}