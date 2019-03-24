package fr.xebia.udaf.bonus1

import fr.xebia.udaf.SparkUtils
import org.scalatest.{FlatSpec, Matchers}

class SkewnessTest extends FlatSpec with Matchers with SparkUtils {

  import spark.implicits._

  "Skewness UDAF" should "correctly compute the skweness in a Dataframe" in {
    // Given
    val df = List(1, 1, 1, 2, 2).toDF("int_col")

    val skewness = new Skewness

    // When
    val result = df.agg(
      skewness($"int_col").as("skewness")
    )

    // Then
    val skewnessValue = result.first.getDouble(0)
    val roundedSkewnessValue = "%.10f".formatLocal(java.util.Locale.US, skewnessValue).toDouble

    roundedSkewnessValue shouldBe 0.4082482905
  }

}
