package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object combinebykey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("combinerbykey")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
//    算出平均成绩
    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("Fred", 88), ("Fred", 95), ("Fred", 91), ("Wilma", 93), ("Wilma", 95), ("Wilma", 98)),3)
    val result: RDD[(String, (Int, Int))] = rdd1.combineByKey(
      //相同分区中的value进来形成一个（88，1）
      (v) => (v, 1),
      //相同分区中的key相同的数字，value相加，并且后面的1相加 例如 ("Fred", 88), ("Fred", 95)
      /*
      * ("Fred", 88), ("Fred", 95) 一个分区   第一步（88，1） 第二部步（88+95，1+1）
      *
      * */
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc2._2 + acc1._2)
    )
    val rsult1: RDD[(String, Double)] = result.map {
      case (key, value) => (key, value._1 / value._2.toDouble)
    }
    rsult1.foreach(print)
  }


}
