package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


class CustomerPartitioner (numParts:Int) extends Partitioner{
//  返回总分区数
  override def numPartitions: Int =numParts
//对应的实际的key
  override def getPartition(key: Any): Int = {
    val ckey: String = key.toString
    ckey.substring(ckey.length-1).toInt%numParts
  }
}

object CustomerPartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("parts")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val data: RDD[String] = sc.parallelize(List("aa.2","bb.2","cc.2","dd.3","dd.3","ee.5"))
//    val size: Int = data.partitions.size
//    print(size)
    val result: RDD[(String, Int)] = data.map((_,1)).partitionBy(new CustomerPartitioner(5))
    val result1: RDD[String] = result.mapPartitionsWithIndex((index, iter) => {
      Iterator(index.toString + ":" + iter.mkString("|"))
    })
    result1.foreach(println)

  }

}
