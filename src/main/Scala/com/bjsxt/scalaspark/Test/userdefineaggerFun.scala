package com.bjsxt.scalaspark.Test

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object userdefineaggerFun extends UserDefinedAggregateFunction{
  // 聚合函数输入参数的数据类型 salary哪一列的一个值的类型
  override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
  // 聚合缓冲区中值得数据类型  有工资的总数 sum 和人员的总数 count  （这个题计算工资的平均值）
//  可以理解为保存为聚合函数业务逻辑数据的一个数据结构
  override def bufferSchema: StructType = {
    StructType(StructField("sum",LongType):: StructField("count",LongType) ::Nil)
  }
  // 返回值的数据类型
  override def dataType: DataType =DoubleType
  // 对于相同的输入是否一直返回相同的输出。 一般都是true
  override def deterministic: Boolean = true
  // 初始化 数据结构
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L//这个存的即使sum
    buffer(1)=0L//这个存的就是count
  }
  // 相同Execute间的数据合并。
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
  if(!input.isNullAt(0)){
    buffer(0)=buffer.getLong(0)+input.getLong(0)
    buffer(1)=buffer.getLong(1)+1
  }

  }
  // 不同Execute间的数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
   buffer1(0)=buffer1.getLong(0)+buffer2.getLong(0)
    buffer1(1)=buffer2.getLong(1)+buffer2.getLong(1)

  }
  // 计算最终结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }


  // 注册函数
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("udaf").getOrCreate()
    val df: DataFrame = spark.read.json("./data/salary")
    df.show()
    df.createOrReplaceTempView("employee")
    spark.udf.register("userdefineaggerFun",userdefineaggerFun)
    spark.sql("select userdefineaggerFun(salary) as average_salary from employee").show()


  }



}
