package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object zipWith {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("appname")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1: RDD[String] = sc.textFile("./data/people.txt")
    val result: RDD[Array[String]] = rdd1.zipWithIndex().map(tuple => {
      val array = new ArrayBuffer[String]()
      array ++= (tuple._1.split("\t"));
      tuple._2.toString +=: array;
      array.toArray
    })
     result.map(_.mkString("\t")).saveAsTextFile("./data/people1")
  }

}
