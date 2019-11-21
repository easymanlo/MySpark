package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mapPartitonIndex {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("flatmapindex")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val rdd1: RDD[(String, String)] = sc.parallelize(List(("kpop","female"),("nihao","male"),("hh","female"),("ni","male")),2)
    def mapPartitionIndexFun(index:Int,iter:Iterator[(String,String)]): Iterator[String] ={
      var woman=List[String]()
      while (iter.hasNext){
        val next=iter.next()
        next match {
          case (_,"female")=>woman="["+index+"]"+next._1 :: woman
          case _=>
        }
      }


        woman.iterator
    }

    val result: RDD[String] = rdd1.mapPartitionsWithIndex(mapPartitionIndexFun)
    val strings: Array[String] = result.collect()
    strings.foreach(println)

  }

}
