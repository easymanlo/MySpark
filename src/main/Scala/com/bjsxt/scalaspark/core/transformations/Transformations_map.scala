package com.bjsxt.scalaspark.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * map 处理数据是一对一的关系
  * 进入一条数据处理，出来的还是一条数据
  *
  * hello
  * spark
  * hello
  * hdfs
  * hello
  * bjsxt
  *
  */
object Transformations_map {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val infos = sc.parallelize(Array[String]("hello spark","hello hdfs","hello bjsxt"))
    val result: RDD[Array[String]] = infos.map(one=>{one.split(" ")})
    result.foreach(arr=>{arr.foreach(println)})
    sc.stop()
  }
}
