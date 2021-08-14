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

    val rowsAdded1 = newArrivingDataOnDifferentAddress
      .withColumn("moved_out", col("new_moved_in"))
      .withColumn("current", lit(false)).drop("new_address", "new_moved_in")
    val rowsFixed = newArrivingDataOnDifferentAddress.withColumn("address", col("new_address"))
      .withColumn("moved_in", col("new_moved_in"))
      .drop("new_address", "new_moved_in")
    val rowsToBeRemoved = newArrivingDataOnDifferentAddress.drop("new_address", "new_moved_in")



    val lateArrivingDataOnDifferentAddress = getLateArrivingDataWithDifferentAddress(joinedHistoryAndUpdate)
    val rowsAdded = lateArrivingDataOnDifferentAddress
      .withColumn("moved_out", col("moved_in"))
      .withColumn("current", lit(false))
      .withColumn("moved_in", col("new_moved_in"))
      .withColumn("address", col("new_address"))
      .drop("new_address", "new_moved_in")



    val lateArrivingDataOnSameAddress = getLateArrivingDataWithSameAddress(joinedHistoryAndUpdate)
    val rowsAdded2 = lateArrivingDataOnSameAddress
      .withColumn("moved_in", col("new_moved_in"))
      .drop("new_address", "new_moved_in")
    val rowsToBeRemoved1 = lateArrivingDataOnSameAddress.drop("new_address", "new_moved_in")



    val newPeopleOnHistory = getNewPeopleOnHistory(update, joinedHistoryAndUpdate)
    val rowsAdded3 = newPeopleOnHistory
      .withColumn("moved_out", lit(null))
      .withColumn("current", lit(true))



    val joinedAdditionWithLeftHouses = getTouchedHistoryForLateArrivingData(history, rowsAdded)

    val updateThatOverlapsWithLateHistory = getTouchedHistoryOfRecordsOverlappingWithUpdates(joinedAdditionWithLeftHouses)

    val rowAdded4 = updateThatOverlapsWithLateHistory
      .withColumn("moved_out" , col("old_moved_out"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
    val rowsAdded5 = updateThatOverlapsWithLateHistory
      .withColumn("moved_out" , col("moved_in"))
      .withColumn("moved_in" , col("old_moved_in"))
      .withColumn("address" , col("old_address"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
    val rowsToBeRemoved2 = updateThatOverlapsWithLateHistory
      .withColumn("moved_in" , col("old_moved_in"))
      .withColumn("moved_out" , col("old_moved_out"))
      .withColumn("address" , col("old_address"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
    val rowsToBeRemoved3 = updateThatOverlapsWithLateHistory
      .drop("old_address" , "old_moved_in" , "old_moved_out")


val updatesOlderThanAllOfHistory = getTouchedHistoryWhenUpdatesAreOlderThanHistory(joinedAdditionWithLeftHouses)
    val rowsAdded6 = updatesOlderThanAllOfHistory
      .withColumn("moved_out" , col("old_moved_in"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
      .sort("moved_out")
      .dropDuplicates("moved_in")
    val rowsToBeRemoved4 = updatesOlderThanAllOfHistory.drop("old_address" , "old_moved_in" , "old_moved_out")



    val additions = rowsAdded2.union(rowsAdded).union(rowsAdded1).union(rowsFixed).union(rowsAdded3).union(rowAdded4).union(rowsAdded5).union(rowsAdded6)
    val toBeRemoved = rowsToBeRemoved.union(rowsToBeRemoved1).union(rowsToBeRemoved2).union(rowsToBeRemoved3).union(rowsToBeRemoved4)
    history.union(additions).except(toBeRemoved)
  }
}
