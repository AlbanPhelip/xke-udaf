package fr.xebia.udaf.example

import fr.xebia.udaf.SparkUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FlatSpec, Matchers}

class AverageTest extends FlatSpec with Matchers with SparkUtils {

  import spark.implicits._

  "Average UDAF" should "correctly compute the average in a Dataframe" in {
    // Given
    val df: DataFrame = List(
      ("a", 2d),
      ("a", 4d),
      ("b", 1d),
      ("b", 3d)
    ).toDF("col_str", "col_double")

    val averageUdaf = new Average()

    // When
    val result = df.groupBy("col_str").agg(averageUdaf($"col_double").as("avg")).collect

    // Then
    result should contain theSameElementsAs Array(
      Row("a", 3d),
      Row("b", 2d)
    )
  }

}
