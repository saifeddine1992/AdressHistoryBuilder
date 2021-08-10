package com.instagram

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, current_date, datediff, lit}


object AddressHistoryBuilder {
  def addressHistoryBuilder(history: DataFrame, update: DataFrame): DataFrame = {
    val renamedUpdateFields = update.withColumnRenamed("address", "new_address")
      .withColumnRenamed("moved_in", "new_moved_in").select("id", "new_address", "new_moved_in")

    val joinedDataFrames = history.join(renamedUpdateFields, "id")

    val case1 = joinedDataFrames.where(col("new_moved_in") > col("moved_in"))
        val rowsFixed = case1
          .withColumn("moved_out", col("new_moved_in"))
          .withColumn("current", lit(false))
          .drop("new_address", "new_moved_in")
        val rowsAdded1 = case1
          .withColumn("address", col("new_address"))
          .withColumn("moved_in", col("new_moved_in"))
          .drop("new_address", "new_moved_in")
        val rowsToBeRemoved = case1.drop("new_address", "new_moved_in")



    val case2 = joinedDataFrames.where(col("new_moved_in")  < col("moved_in"))
    val rowsAdded = case2
          .withColumn("moved_out", col("moved_in"))
          .withColumn("current", lit(false))
          .withColumn("moved_in", col("new_moved_in"))
          .withColumn("address", col("new_address"))
          .drop("new_address", "new_moved_in")
        val output = history.union(rowsAdded)
        output.union(rowsFixed).union(rowsAdded1).except(rowsToBeRemoved)


  }
}
