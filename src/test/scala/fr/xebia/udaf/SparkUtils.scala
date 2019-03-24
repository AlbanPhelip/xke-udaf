package fr.xebia.udaf

import org.apache.spark.sql.SparkSession

trait SparkUtils {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("UDAF XKE")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
