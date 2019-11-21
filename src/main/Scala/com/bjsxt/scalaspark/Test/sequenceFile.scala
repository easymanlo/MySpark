package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object sequenceFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("sequenceFile")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val result: RDD[(Int, String)] = sc.parallelize(List((2,"aa"),(3,"bb"),(4,"cc"),(5,"dd"),(6,"ee")))
//    result.saveAsSequenceFile("hdfs://node001:8020/test")
    val result1: RDD[(Int, String)] = sc.sequenceFile[Int,String]("hdfs://node001:8020/test/p*")
    result1.collect().foreach(println)

  }

}
