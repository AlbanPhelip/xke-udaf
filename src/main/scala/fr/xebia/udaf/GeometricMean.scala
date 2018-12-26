package fr.xebia.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class GeometricMean extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
      StructField("product", DoubleType) :: Nil
  )

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0l
    buffer(1) = 0d
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = buffer.getAs[Double](1) * input.getAs[Double](0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  override def evaluate(buffer: Row): Any = {
    scala.math.pow(buffer.getAs[Double](1), 1d / buffer.getAs[Long](0))
  }
}
