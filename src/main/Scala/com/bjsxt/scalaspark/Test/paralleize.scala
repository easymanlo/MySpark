package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object paralleize {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("paralleize")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))
    val list = List((1, List("hello", "world", "nihao")), (2, List("zhangsan", "lisi")))
    val rdd2: RDD[Int] = sc.makeRDD(list)
    /*
    * makeRdd可以指定分区位置
    * */
    val result: Seq[String] = rdd2.preferredLocations(rdd2.partitions(0))
    println(result)


  }


}
