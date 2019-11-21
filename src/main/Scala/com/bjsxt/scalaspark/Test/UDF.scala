package com.bjsxt.scalaspark.Test

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

object UDF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("udf").master("local").getOrCreate()
    val df: DataFrame = spark.read.json("./data/json")
    df.show()
    val userFun: UserDefinedFunction = spark.udf.register("addname",(x:String)=>"Name:"+x)
   df.createOrReplaceTempView("people")
    spark.sql("select addname(name) as name,age from people").show()
  }

}
