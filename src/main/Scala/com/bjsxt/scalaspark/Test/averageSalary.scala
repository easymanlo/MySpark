package com.bjsxt.scalaspark.Test

import com.atguigu.spark.{Average, Employee, MyAverage}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator
case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)


