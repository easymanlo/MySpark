package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object paratitionBy {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("mapParition")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1: RDD[(Int, String)] = sc.parallelize(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
    val size: Int = rdd1.partitions.size
    print(size)
    val result: RDD[(Int, String)] = rdd1.partitionBy(new HashPartitioner(2))
    val size1: Int = result.partitions.size
    print(size1)

  }

}
