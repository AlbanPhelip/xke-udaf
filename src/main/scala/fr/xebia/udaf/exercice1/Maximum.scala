package fr.xebia.udaf.exercice1

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, StructField, StructType}

class Maximum extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("max", DoubleType) :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Double.MinValue
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(input.getDouble(0) > buffer.getDouble(0)) buffer(0) = input.getDouble(0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(buffer2.getDouble(0) > buffer1.getDouble(0)) buffer1(0) = buffer2.getDouble(0)
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)
  }
}
