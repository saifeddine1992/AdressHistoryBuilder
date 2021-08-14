package com.instagram

import com.instagram.TouchedHistory.{ getLateArrivingDataWithDifferentAddress,
  getLateArrivingDataWithSameAddress, getNewArrivingDataWithDifferentAddress,
  getNewPeopleOnHistory, getTouchedHistoryForLateArrivingData, getTouchedHistoryForNewArrivingData
  , getTouchedHistoryOfRecordsOverlappingWithUpdates, getTouchedHistoryWhenUpdatesAreOlderThanHistory}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}


object AddressHistoryBuilder {
  def addressHistoryBuilder (history: DataFrame, update: DataFrame): DataFrame = {

    val joinedHistoryAndUpdate = getTouchedHistoryForNewArrivingData(history, update)
    val newArrivingDataOnDifferentAddress = getNewArrivingDataWithDifferentAddress(joinedHistoryAndUpdate)

    val rowsAddedOnDifferentAddress = newArrivingDataOnDifferentAddress
      .withColumn("moved_out", col("new_moved_in"))
      .withColumn("current", lit(false)).drop("new_address", "new_moved_in")
    val newCurrentRowsAdded = newArrivingDataOnDifferentAddress.withColumn("address", col("new_address"))
      .withColumn("moved_in", col("new_moved_in"))
      .drop("new_address", "new_moved_in")
    val OldCurrentRowsToBeRemoved = newArrivingDataOnDifferentAddress.drop("new_address", "new_moved_in")



    val lateArrivingDataOnDifferentAddress = getLateArrivingDataWithDifferentAddress(joinedHistoryAndUpdate)
    val PotentiallyAddedRows = lateArrivingDataOnDifferentAddress
      .withColumn("moved_out", col("moved_in"))
      .withColumn("current", lit(false))
      .withColumn("moved_in", col("new_moved_in"))
      .withColumn("address", col("new_address"))
      .drop("new_address", "new_moved_in")



    val lateArrivingDataOnSameAddress = getLateArrivingDataWithSameAddress(joinedHistoryAndUpdate)
    val rowsAddedOnSameAddress = lateArrivingDataOnSameAddress
      .withColumn("moved_in", col("new_moved_in"))
      .drop("new_address", "new_moved_in")
    val rowsRemovedOnSameAddress = lateArrivingDataOnSameAddress.drop("new_address", "new_moved_in")



    val newPeopleOnHistory = getNewPeopleOnHistory(update, joinedHistoryAndUpdate)
    val newPeopleRows = newPeopleOnHistory
      .withColumn("moved_out", lit(null))
      .withColumn("current", lit(true))



    val joinedAdditionWithLeftHouses = getTouchedHistoryForLateArrivingData(history, PotentiallyAddedRows)

    val updateThatOverlapsWithLateHistory = getTouchedHistoryOfRecordsOverlappingWithUpdates(joinedAdditionWithLeftHouses)

    val rowsOfUpdateAdded = updateThatOverlapsWithLateHistory
      .withColumn("moved_out" , col("old_moved_out"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
    val rowsOfHistoryCorrected = updateThatOverlapsWithLateHistory
      .withColumn("moved_out" , col("moved_in"))
      .withColumn("moved_in" , col("old_moved_in"))
      .withColumn("address" , col("old_address"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
    val OldRowsOfHistoryRemoved = updateThatOverlapsWithLateHistory
      .withColumn("moved_in" , col("old_moved_in"))
      .withColumn("moved_out" , col("old_moved_out"))
      .withColumn("address" , col("old_address"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
    val PotentiallyAddedRowsWhenOverlapping = updateThatOverlapsWithLateHistory
      .drop("old_address" , "old_moved_in" , "old_moved_out")


val updatesOlderThanAllOfHistory = getTouchedHistoryWhenUpdatesAreOlderThanHistory(joinedAdditionWithLeftHouses)
    val OldestRowsAdded = updatesOlderThanAllOfHistory
      .withColumn("moved_out" , col("old_moved_in"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
      .sort("moved_out")
      .dropDuplicates("moved_in")
    val PotentiallyAddedRowsWhenOldestRecord = updatesOlderThanAllOfHistory
      .drop("old_address" , "old_moved_in" , "old_moved_out")


    val additions = rowsAddedOnDifferentAddress
      .union(newCurrentRowsAdded)
      .union(PotentiallyAddedRows)
      .union(rowsAddedOnSameAddress)
      .union(newPeopleRows)
      .union(rowsOfUpdateAdded)
      .union(rowsOfHistoryCorrected)
      .union(OldestRowsAdded)
    val toBeRemoved = OldCurrentRowsToBeRemoved
      .union(rowsRemovedOnSameAddress)
      .union(OldRowsOfHistoryRemoved)
      .union(PotentiallyAddedRowsWhenOverlapping)
      .union(PotentiallyAddedRowsWhenOldestRecord)
    history.union(additions).except(toBeRemoved)
  }
}
