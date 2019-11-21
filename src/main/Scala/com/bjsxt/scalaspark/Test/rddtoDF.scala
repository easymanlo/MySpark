package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

case class person(var name:String,var age:Int)
object rddtoDS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val rdd1: RDD[String] = spark.sparkContext.textFile("./data/peop.txt")
    val df: DataFrame = rdd1.map(_.split(",")).map(attributes=>person(attributes(0),attributes(1).trim().toInt)).toDF
//    df.show()
    df.createOrReplaceTempView("persons")
    spark.sql("select * from persons where age>18").show()
  }
}
