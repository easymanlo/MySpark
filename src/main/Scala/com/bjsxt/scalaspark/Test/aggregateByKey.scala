package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object aggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("aggregateByKey")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1: RDD[(Int, Int)] = sc.parallelize(List((1,3),(1,2),(1,4),(2,3),(3,6),(3,8)),3)
    val result: RDD[(Int, Int)] = rdd1.aggregateByKey(0)(math.max(_,_),_+_)
    val tuples: Array[(Int, Int)] = result.collect()
    tuples.foreach(println)
    val size: Int = result.partitions.size
    print(size)

  }

}
