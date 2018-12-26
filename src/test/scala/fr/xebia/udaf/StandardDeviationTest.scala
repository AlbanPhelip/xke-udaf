package fr.xebia.udaf

import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.functions

class StandardDeviationTest extends FlatSpec with Matchers with SparkUtils {

  import spark.implicits._

  "StandardDeviation UDAF" should "compute de standard deviation" in {
    // Given
    val df: DataFrame = List(1, 2, 3, 4, 5).toDF("col")
    val standardDeviationUdaf = new StandardDeviation()

    // When
    val result = df.agg(standardDeviationUdaf($"col"), functions.stddev($"col"))

    // Then
    result.first.getDouble(0) shouldBe 1.5811388300841898
  }

  "Standard deviation" should "be NaN when there is only one element" in {
    // Given
    val df: DataFrame = List(1).toDF("col")
    val standardDeviationUdaf = new StandardDeviation()

    // When
    val result = df.agg(standardDeviationUdaf($"col"), functions.stddev($"col"))

    // Then
    assert(result.first.getDouble(0).isNaN)
  }

}
