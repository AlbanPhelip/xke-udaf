package fr.xebia.udaf.exercice3

import fr.xebia.udaf.SparkUtils
import org.apache.spark.sql.{DataFrame, functions}
import org.scalatest.{FlatSpec, Matchers}

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
