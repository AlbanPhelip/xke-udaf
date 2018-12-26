package fr.xebia.udaf

import org.apache.spark.sql.SparkSession

trait SparkUtils {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("toto")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
