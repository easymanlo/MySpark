package com.bjsxt.scalaspark.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object combinerByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("combinerbykey")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val input: RDD[(String, Int)] = sc.makeRDD(List[(String, Int)](("panda", 0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4)))
    val result = input.combineByKey(
      (v) => (v, 1),
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
     //(pirate,3.0)
    //(pink,3.5)
    //(panda,0.5)
    result.collectAsMap().map(println(_))

  }


}
