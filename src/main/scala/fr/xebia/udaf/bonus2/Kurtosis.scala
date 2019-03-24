package fr.xebia.udaf.bonus2

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.math.pow

class Kurtosis extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
      StructField("sum", DoubleType) ::
      StructField("squared_sum", DoubleType) ::
      StructField("cubed_sum", DoubleType) ::
      StructField("fourth_sum", DoubleType) :: Nil
  )

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0l
    buffer(1) = 0d
    buffer(2) = 0d
    buffer(3) = 0d
    buffer(4) = 0d
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + 1
    buffer(1) = buffer.getDouble(1) + input.getDouble(0)
    buffer(2) = buffer.getDouble(2) + pow(input.getDouble(0), 2)
    buffer(3) = buffer.getDouble(3) + pow(input.getDouble(0), 3)
    buffer(4) = buffer.getDouble(4) + pow(input.getDouble(0), 4)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
    buffer1(2) = buffer1.getDouble(2) + buffer2.getDouble(2)
    buffer1(3) = buffer1.getDouble(3) + buffer2.getDouble(3)
    buffer1(4) = buffer1.getDouble(4) + buffer2.getDouble(4)
  }

  override def evaluate(buffer: Row): Any = {
    val n = buffer.getLong(0).toDouble
    val X = buffer.getDouble(1)
    val X2 = buffer.getDouble(2)
    val X3 = buffer.getDouble(3)
    val X4 = buffer.getDouble(4)
    val mean = X / n
    val m2 = X2 - 2 * mean * X + n * pow(mean, 2)
    val m4 = X4 - 4*mean*X3 + 6*pow(mean,2)*X2 - 4*pow(mean,3)*X + n*pow(mean,4)

    n * m4 / (m2 * m2) - 3
  }


}