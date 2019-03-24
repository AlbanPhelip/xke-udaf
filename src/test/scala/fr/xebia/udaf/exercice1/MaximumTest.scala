package fr.xebia.udaf.exercice1

import fr.xebia.udaf.SparkUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class MaximumTest extends FlatSpec with Matchers with SparkUtils {

  import spark.implicits._

  "Maximum UDAF" should "correctly compute the maximum in a Dataframe" in {
    // Given
    val df: DataFrame = List(2d, 4d, 1d, 3d).toDF("col_double")

    val maximum = new Maximum()

    // When
    val result = df.agg(maximum($"col_double").as("max"))

    // Then
    result.first.getDouble(0) shouldBe 4d
  }


}
