package com.instagram

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}


object AddressHistoryBuilder {
  def addressHistoryBuilder(history: DataFrame, update: DataFrame): DataFrame = {
    val renamedUpdateFields = update.withColumnRenamed("address", "new_address")
      .withColumnRenamed("moved_in", "new_moved_in").select("id", "new_address", "new_moved_in")
    val historyThatMatters = history.where(col("current") === true)

    val historyWithHousesLeft = history.where(col("current") === false)
      .withColumnRenamed("moved_out" , "old_moved_out")
      .withColumnRenamed("moved_in" , "old_moved_in")
      .withColumnRenamed("address" , "old_address")
      .select("id" , "old_address" , "old_moved_in" , "old_moved_out")


    val joinedHistoryAndUpdate = historyThatMatters.join(renamedUpdateFields, "id")


    val newRecordAddedOnDifferentAdress = joinedHistoryAndUpdate.where(col("new_address") =!= col("address"))

    val recordAddedOnDifferentAdressWithDateCondition1 = newRecordAddedOnDifferentAdress.where(col("new_moved_in") > col("moved_in"))

    val rowsAdded1 = recordAddedOnDifferentAdressWithDateCondition1
      .withColumn("moved_out", col("new_moved_in"))
      .withColumn("current", lit(false))
      .drop("new_address", "new_moved_in")

    val rowsFixed = recordAddedOnDifferentAdressWithDateCondition1.withColumn("address", col("new_address"))
      .withColumn("moved_in", col("new_moved_in"))
      .drop("new_address", "new_moved_in")
    val rowsToBeRemoved = recordAddedOnDifferentAdressWithDateCondition1.drop("new_address", "new_moved_in")


    val recordAddedOnDifferentAdressWithDateCondition2 = newRecordAddedOnDifferentAdress.where(col("new_moved_in") < col("moved_in"))
    val rowsAdded = recordAddedOnDifferentAdressWithDateCondition2
      .withColumn("moved_out", col("moved_in"))
      .withColumn("current", lit(false))
      .withColumn("moved_in", col("new_moved_in"))
      .withColumn("address", col("new_address"))
      .drop("new_address", "new_moved_in")


    val recordAddedOnSameAdress = joinedHistoryAndUpdate.where(col("new_address") === col("address"))
    val recordAddedOnSameAdressWithDateCondition = recordAddedOnSameAdress.where(col("new_moved_in") < col("moved_in"))

    val rowsAdded2 = recordAddedOnSameAdressWithDateCondition
      .withColumn("moved_in", col("new_moved_in"))
      .drop("new_address", "new_moved_in")
    val rowsToBeRemoved1 = recordAddedOnSameAdressWithDateCondition.drop("new_address", "new_moved_in")


    val newRecordsToBeAdded = update.join(historyThatMatters, historyThatMatters("id") === update("id"), "leftanti")
    val addedRecord = newRecordsToBeAdded.withColumn("moved_out", lit(null)).withColumn("current", lit(true))



    val joinedAdditionWithLeftHouses = rowsAdded.join(historyWithHousesLeft , "id")
    val interval1 = joinedAdditionWithLeftHouses.where(col("moved_in") > col("old_moved_in"))
    val case1 = interval1.where(col("moved_in") < col("old_moved_out"))
    val rowAddedd1 = case1.withColumn("moved_out" , col("old_moved_out"))
      .drop("old_address" , "old_moved_in" , "old_moved_out")
    val rowsAddedd2 = case1
      .withColumn("moved_out" , col("moved_in"))
      .withColumn("moved_in" , col("old_moved_in"))
      .withColumn("address" , col("old_address")).drop("old_address" , "old_moved_in" , "old_moved_out")
    val rowtoberemovedd = case1.withColumn("moved_in" , col("old_moved_in"))
      .withColumn("moved_out" , col("old_moved_out"))
      .withColumn("address" , col("old_address")).drop("old_address" , "old_moved_in" , "old_moved_out")
    val rowstoberemovedd1 = case1.drop("old_address" , "old_moved_in" , "old_moved_out")




    val additions = rowsAdded2.union(rowsAdded).union(rowsAdded1).union(rowsFixed).union(addedRecord).union(rowAddedd1).union(rowsAddedd2)
    val toBeRemoved = rowsToBeRemoved.union(rowsToBeRemoved1).union(rowtoberemovedd).union(rowstoberemovedd1)
    history.union(additions).except(toBeRemoved)
  }
}
