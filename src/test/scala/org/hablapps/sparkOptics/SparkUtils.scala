package org.hablapps.sparkOptics

import org.apache.spark.sql.SparkSession

object SparkUtils {
  private val sparkSession: SparkSession = {
    val ss = SparkSession.builder().appName("test").master("local[1]").getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    ss
  }
}

trait SparkUtils {
  val sparkSession: SparkSession = SparkUtils.sparkSession
}
