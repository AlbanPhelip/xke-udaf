package fr.xebia.udaf

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class Average extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
    StructField("sum", DoubleType) :: Nil
  )

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0l
    buffer(1) = 0d
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + 1
    buffer(1) = buffer.getDouble(1) + input.getDouble(0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(1) / buffer.getLong(0)
  }
}
