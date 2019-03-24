package fr.xebia.udaf.bonus2

import fr.xebia.udaf.SparkUtils
import org.scalatest.{FlatSpec, Matchers}

class KurtosisTest extends FlatSpec with Matchers with SparkUtils {

  import spark.implicits._

  "Kurtosis UDAF" should "correctly compute the kurtosis in a Dataframe" in {
    // Given
    val df = List(1, 1, 1, 2, 2).toDF("int_col")

    val kurtosis = new Kurtosis

    // When
    val result = df.agg(
      kurtosis($"int_col").as("kurtosis")
    )

    // Then
    val kurtosisValue = result.first.getDouble(0)
    val roundedKurtosisValue = "%.10f".formatLocal(java.util.Locale.US, kurtosisValue).toDouble

    roundedKurtosisValue shouldBe -1.8333333333
  }

}
