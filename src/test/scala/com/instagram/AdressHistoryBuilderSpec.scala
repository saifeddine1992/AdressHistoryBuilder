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


class AddressHistoryBuilderSpec  extends AnyFlatSpec with Matchers with GivenWhenThen {
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("History builer App")
    .getOrCreate()

  import spark.implicits._

  val pattern = DateTimeFormatter.ofPattern("dd-MM-yyyy")
  val adressHistory = Seq(AddressHistory(1, "Sayf", "Bouazizi", "Kasserine", LocalDate.parse("21-11-1992", pattern), null, true)).toDF()
  val historyUpdate = Seq(addressUpdates(1, "Sayf", "Bouazizi", "Sousse", LocalDate.parse("06-06-2017", pattern))).toDF()
  val expectedResult = Seq(
    AddressHistory(1, "Sayf", "Bouazizi", "Sousse", LocalDate.parse("06-06-2017", pattern), null , true),
    AddressHistory(1,"Sayf", "Bouazizi", "Kasserine", LocalDate.parse("21-11-1992", pattern), LocalDate.parse("06-06-2017", pattern), false)
  ).toDF()

  "AddressHistoryBuilder" should "update the address history when given an update" in {
    Given("the address history and the update")
    val History = adressHistory
    val update = historyUpdate
    When("AddressHistoryBuilder is Invoked")
    val result = addressHistoryBuilder(History, update)
    Then("the address history should be updated")
    expectedResult.collect() should contain theSameElementsAs(result.collect())
  }
}