package com.bjsxt.scalaspark.Test

import org.apache.spark.sql.{DataFrame, SparkSession}

object dataFrame {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("d").master("local").getOrCreate()
    val df: DataFrame = spark.read.json("./data/json")
//    import spark.implicits._
  df.show()

//  df.filter($"age">18).show()
    df.createOrReplaceTempView("persons")
    spark.sql("select * from persons where age>18 ").show()
    spark.stop()



  }

}
