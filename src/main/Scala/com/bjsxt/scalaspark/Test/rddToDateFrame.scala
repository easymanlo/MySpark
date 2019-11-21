package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object rddToDateFrame {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("rddtoDateFrame").master("local").getOrCreate()
//     val conf = new SparkConf()
//    conf.setMaster("local")
//    conf.setAppName("rddTodateframe")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
    import spark.implicits._
    val rdd1: RDD[String] = spark.sparkContext.textFile("./data/peop.txt")
     val df: DataFrame = rdd1.map(_.split(",")).map(paras=>(paras(0),paras(1).trim().toInt)).toDF("name","age")
    df.show()

//    rdd1.map(_.split(",")).map(paras => (paras(0),paras(1).trim().toInt)).toDF("name","age")
  }

}
