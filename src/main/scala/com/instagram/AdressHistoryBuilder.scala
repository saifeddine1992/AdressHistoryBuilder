package com.instagram

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}


object AddressHistoryBuilder {
  def addressHistoryBuilder(history: DataFrame, update: DataFrame): DataFrame = {
    val renamedUpdateFields = update.withColumnRenamed("address", "new_address")
      .withColumnRenamed("moved_in", "new_moved_in").select("id", "new_address", "new_moved_in")

    val joinedHistoryAndUpdate = history.where(col("current") === true).join(renamedUpdateFields, "id")

    val newRecordAddedOnDifferentAdress = joinedHistoryAndUpdate.where(col("new_moved_in") > col("moved_in"))
        val rowsFixed = newRecordAddedOnDifferentAdress
          .withColumn("moved_out", col("new_moved_in"))
          .withColumn("current", lit(false))
          .drop("new_address", "new_moved_in")
        val rowsAdded1 = newRecordAddedOnDifferentAdress
          .withColumn("address", col("new_address"))
          .withColumn("moved_in", col("new_moved_in"))
          .drop("new_address", "new_moved_in")
        val rowsToBeRemoved = newRecordAddedOnDifferentAdress.drop("new_address", "new_moved_in")



    val oldRecordAddedOnDifferentAdress = joinedHistoryAndUpdate.where(col("new_moved_in")  < col("moved_in"))
    val rowsAdded = oldRecordAddedOnDifferentAdress
          .withColumn("moved_out", col("moved_in"))
          .withColumn("current", lit(false))
          .withColumn("moved_in", col("new_moved_in"))
          .withColumn("address", col("new_address"))
          .drop("new_address", "new_moved_in")
        val output = history.union(rowsAdded)
        output.union(rowsFixed).union(rowsAdded1).except(rowsToBeRemoved)


  }
}
