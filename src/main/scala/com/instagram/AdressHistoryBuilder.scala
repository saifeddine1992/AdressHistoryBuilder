package com.instagram

import com.instagram.TouchedHistory._
import org.apache.spark.sql.DataFrame


object AddressHistoryBuilder {
  def addressHistoryBuilder(history: DataFrame, update: DataFrame): DataFrame = {

    val joinedHistoryAndUpdate = getTouchedHistoryForNewArrivingData(history, update)
    val newArrivingDataOnDifferentAddress = getNewArrivingDataWithDifferentAddress(joinedHistoryAndUpdate)
    val rowsAddedOnDifferentAddress = getAddedOnDifferentAddress(newArrivingDataOnDifferentAddress)
    val newCurrentRowsAdded = getNewCurrentRowsAdded(newArrivingDataOnDifferentAddress)
    val OldCurrentRowsToBeRemoved = getOldCurrentRowsToBeRemoved(newArrivingDataOnDifferentAddress)
    val lateArrivingDataOnDifferentAddress = getLateArrivingDataWithDifferentAddress(joinedHistoryAndUpdate)
    val PotentiallyAddedRows = getPotentiallyAddedRows(lateArrivingDataOnDifferentAddress)
    val lateArrivingDataOnSameAddress = getLateArrivingDataWithSameAddress(joinedHistoryAndUpdate)
    val rowsAddedOnSameAddress = getRowsAddedOnSameAddress(lateArrivingDataOnSameAddress)
    val rowsRemovedOnSameAddress = getRowsRemovedOnSameAddress(lateArrivingDataOnSameAddress)
    val newPeopleOnHistory = getNewPeopleOnHistory(update, joinedHistoryAndUpdate)
    val newPeopleRows = buildNewPeopleRows(newPeopleOnHistory)
    val joinedAdditionWithLeftHouses = getTouchedHistoryForLateArrivingData(history, PotentiallyAddedRows)
    val updateThatOverlapsWithLateHistory = getTouchedHistoryOfRecordsOverlappingWithUpdates(joinedAdditionWithLeftHouses)
    val rowsOfUpdateAdded = buildRowsOfUpdateAdded(updateThatOverlapsWithLateHistory)
    val rowsOfHistoryCorrected = buildRowsOfHistoryCorrected(updateThatOverlapsWithLateHistory)
    val OldRowsOfHistoryRemoved = buildOldRowsOfHistoryRemoved(updateThatOverlapsWithLateHistory)
    val PotentiallyAddedRowsWhenOverlapping = buildPotentiallyAddedRowsWhenOverlapping(updateThatOverlapsWithLateHistory)
    val updatesOlderThanAllOfHistory = getTouchedHistoryWhenUpdatesAreOlderThanHistory(joinedAdditionWithLeftHouses)
    val OldestRowsAdded = getOldestRowsAdded(updatesOlderThanAllOfHistory)
    val PotentiallyAddedRowsWhenOldestRecord = getPotentiallyAddedRowsWhenOldestRecord(updatesOlderThanAllOfHistory)

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
