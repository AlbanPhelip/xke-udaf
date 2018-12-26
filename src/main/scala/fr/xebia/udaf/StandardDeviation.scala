package fr.xebia.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import scala.math._

class StandardDeviation extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

  override def bufferSchema: StructType =  StructType(
    StructField("count", LongType) ::
      StructField("sum", DoubleType) ::
      StructField("squared_sum", DoubleType) :: Nil
  )

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0l
    buffer(1) = 0d
    buffer(2) = 0d
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + 1
    buffer(1) = buffer.getDouble(1) + input.getDouble(0)
    buffer(2) = buffer.getDouble(2) + pow(input.getDouble(0), 2)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
    buffer1(2) = buffer1.getDouble(2) + buffer2.getDouble(2)
  }

  override def evaluate(buffer: Row): Any = {
    val n = buffer.getLong(0)
    val sum = buffer.getDouble(1)
    val mean = sum / n

    sqrt((1d / (n - 1)) * (buffer.getDouble(2) - 2 * mean * sum + n * pow(mean, 2)))
  }
}
