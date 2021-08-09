package com.instagram

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

object AddressHistoryBuilder {
  def addressHistoryBuilder(history: DataFrame, update: DataFrame): DataFrame = {
    val renamedUpdateFields = update.withColumnRenamed("address", "new_address")
      .withColumnRenamed("moved_in", "new_moved_in").select("id", "new_address", "new_moved_in")

    val joinedDataFrames = history.join(renamedUpdateFields, "id")
    val rowsFixed = joinedDataFrames.withColumn("moved_out", col("new_moved_in"))
      .withColumn("current", lit(false)).drop("new_address", "new_moved_in")
    val rowsAdded = joinedDataFrames.withColumn("address", col("new_address"))
      .withColumn("moved_in", col("new_moved_in")).drop("new_address", "new_moved_in")
    val rowsToBeRemoved = joinedDataFrames.drop("new_address", "new_moved_in")

    val output = history.except(rowsToBeRemoved).union(rowsFixed.union(rowsAdded))
    output
  }
}
