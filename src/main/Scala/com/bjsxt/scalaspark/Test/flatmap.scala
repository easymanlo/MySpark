package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object flatmap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("flatmap")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4,5))
    val result: Array[Int] = rdd1.collect()
    val result1: RDD[Int] = rdd1.flatMap(1 to _)
    result1.collect().foreach(println)
  }

}
