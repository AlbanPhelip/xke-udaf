package fr.xebia.udaf.exercice2

import fr.xebia.udaf.SparkUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.{FlatSpec, Matchers}

class GeometricMeanTest extends FlatSpec with Matchers with SparkUtils {

  import spark.implicits._

  "GeometricMean UDAF" should "correctly compute the geometric mean in a Dataframe" in {
    // Given
    val df: DataFrame = List(2d, 4d, 1d, 3d).toDF("col_double")

    val gm = new GeometricMean()

    // When
    val result = df.agg(gm($"col_double").as("gm"))

    // Then
    result.first.getDouble(0) shouldBe 2.2133638394006434
  }


}
