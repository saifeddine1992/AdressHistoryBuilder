package com.instagram

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

object TouchedHistory {
  def getTouchedHistoryForNewArrivingData(history: DataFrame, update: DataFrame): DataFrame = {
    val renamedUpdateFields = update
      .select(
        col("id"),
        col("address") as "new_address",
        col("moved_in") as "new_moved_in"
      )
    val historyThatMatters = history.where(col("current") === true)
    val joinedHistoryAndUpdate = historyThatMatters.join(renamedUpdateFields, "id")
    joinedHistoryAndUpdate
  }
  def getTouchedHistoryForLateArrivingData(history: DataFrame, rowsAdded: DataFrame): DataFrame = {

    val historyWithHousesLeft = history.where(col("current") === false)
      .withColumnRenamed("moved_out", "old_moved_out")
      .withColumnRenamed("moved_in", "old_moved_in")
      .withColumnRenamed("address", "old_address")
      .select("id", "old_address", "old_moved_in", "old_moved_out")
    val joinedAdditionWithLeftHouses = rowsAdded.join(historyWithHousesLeft, "id")
    joinedAdditionWithLeftHouses
  }
  def getNewArrivingDataWithDifferentAddress(joinedHistoryAndUpdate: DataFrame): DataFrame = {

    val newRecordAddedOnDifferentAdress = joinedHistoryAndUpdate.where(col("new_address") =!= col("address"))
    val recordAddedOnDifferentAdressWithDateCondition1 = newRecordAddedOnDifferentAdress.where(col("new_moved_in") > col("moved_in"))
    recordAddedOnDifferentAdressWithDateCondition1
  }
  def getLateArrivingDataWithDifferentAddress(joinedHistoryAndUpdate: DataFrame): DataFrame = {
    val newRecordAddedOnDifferentAdress = joinedHistoryAndUpdate.where(col("new_address") =!= col("address"))
    val lateArrivingDataOnDifferentAddress = newRecordAddedOnDifferentAdress
      .where(col("new_moved_in") < col("moved_in"))
    lateArrivingDataOnDifferentAddress
  }
  def getLateArrivingDataWithSameAddress(joinedHistoryAndUpdate: DataFrame): DataFrame = {
    val recordAddedOnSameAdress = joinedHistoryAndUpdate
      .where(col("new_address") === col("address"))
    val lateArrivingDataOnSameAddress = recordAddedOnSameAdress
      .where(col("new_moved_in") < col("moved_in"))
    lateArrivingDataOnSameAddress
  }
  def getNewPeopleOnHistory(update: DataFrame, joinedHistoryAndUpdate: DataFrame): DataFrame = {
    val newRecordsToBeAdded = update.join(joinedHistoryAndUpdate, joinedHistoryAndUpdate("id") === update("id"), "leftanti")
    newRecordsToBeAdded
  }

  def getTouchedHistoryOfRecordsOverlappingWithUpdates(joinedAdditionWithLeftHouses : DataFrame ) : DataFrame = {
    val interval1 = joinedAdditionWithLeftHouses.where(col("moved_in") > col("old_moved_in"))
    val overlappingDates = interval1.where(col("moved_in") < col("old_moved_out"))
    overlappingDates
  }

  def getTouchedHistoryWhenUpdatesAreOlderThanHistory(joinedAdditionWithLeftHouses : DataFrame) : DataFrame = {
    val updatesOlderThanAllOfHistory = joinedAdditionWithLeftHouses.where(col("moved_in") < col("old_moved_in"))
    updatesOlderThanAllOfHistory
  }
  def getAddedOnDifferentAddress(newArrivingDataOnDifferentAddress:DataFrame ): DataFrame = {
    newArrivingDataOnDifferentAddress
      .withColumn("moved_out", col("new_moved_in"))
      .withColumn("current", lit(false))
      .drop("new_address", "new_moved_in")
  }
  def getNewCurrentRowsAdded(newArrivingDataOnDifferentAddress:DataFrame) : DataFrame = {
    newArrivingDataOnDifferentAddress
      .withColumn("address", col("new_address"))
      .withColumn("moved_in", col("new_moved_in"))
      .drop("new_address", "new_moved_in")
  }
  def getOldCurrentRowsToBeRemoved(newArrivingDataOnDifferentAddress:DataFrame): DataFrame = {
    newArrivingDataOnDifferentAddress
      .drop("new_address", "new_moved_in")
  }
  def getPotentiallyAddedRows(lateArrivingDataOnDifferentAddress : DataFrame) : DataFrame = {
    lateArrivingDataOnDifferentAddress
      .withColumn("moved_out", col("moved_in"))
      .withColumn("current", lit(false))
      .withColumn("moved_in", col("new_moved_in"))
      .withColumn("address", col("new_address"))
      .drop("new_address", "new_moved_in")
  }
  def getRowsAddedOnSameAddress(lateArrivingDataOnSameAddress : DataFrame): DataFrame = {
    lateArrivingDataOnSameAddress
      .withColumn("moved_in", col("new_moved_in"))
      .drop("new_address", "new_moved_in")
  }
  def getRowsRemovedOnSameAddress(lateArrivingDataOnSameAddress : DataFrame) : DataFrame = {
    lateArrivingDataOnSameAddress
      .drop("new_address", "new_moved_in")
  }
  def buildNewPeopleRows(newPeopleOnHistory: DataFrame): DataFrame = {
    newPeopleOnHistory
      .withColumn("moved_out", lit(null))
      .withColumn("current", lit(true))
  }
  def buildRowsOfUpdateAdded(updateThatOverlapsWithLateHistory: DataFrame): DataFrame = {
    updateThatOverlapsWithLateHistory
      .withColumn("moved_out" , col("old_moved_out"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
  }
  def buildRowsOfHistoryCorrected(updateThatOverlapsWithLateHistory: DataFrame): DataFrame={
    updateThatOverlapsWithLateHistory
      .withColumn("moved_out" , col("moved_in"))
      .withColumn("moved_in" , col("old_moved_in"))
      .withColumn("address" , col("old_address"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
  }
  def buildOldRowsOfHistoryRemoved(updateThatOverlapsWithLateHistory: DataFrame): DataFrame = {
    updateThatOverlapsWithLateHistory
      .withColumn("moved_in" , col("old_moved_in"))
      .withColumn("moved_out" , col("old_moved_out"))
      .withColumn("address" , col("old_address"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
  }
  def buildPotentiallyAddedRowsWhenOverlapping(updateThatOverlapsWithLateHistory: DataFrame): DataFrame = {
    updateThatOverlapsWithLateHistory
      .drop("old_address" , "old_moved_in" , "old_moved_out")
  }
  def getOldestRowsAdded(updatesOlderThanAllOfHistory : DataFrame) :DataFrame = {
    updatesOlderThanAllOfHistory
      .withColumn("moved_out" , col("old_moved_in"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
      .sort("moved_out")
      .dropDuplicates("moved_in")
  }
  def getPotentiallyAddedRowsWhenOldestRecord(updatesOlderThanAllOfHistory : DataFrame): DataFrame = {
    updatesOlderThanAllOfHistory
      .drop("old_address" , "old_moved_in" , "old_moved_out")
  }

}
