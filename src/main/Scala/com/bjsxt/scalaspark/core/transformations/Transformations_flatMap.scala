package com.bjsxt.scalaspark.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * flatMap 是一对多的关系
  * 处理一条数据得到多条数据结果
  */
object Transformations_flatMap {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("map").setMaster("local")
    val sc = new SparkContext(conf)
//    val infos = sc.parallelize(Array[String]("hello spark","hello hdfs","hello bjsxt"))
      val infos = sc.parallelize(Array[String]("hello spark","hello hdfs","hello bjsxt"))

    val value: RDD[Int] = sc.parallelize(Array[Int](1,1,2,3,4,2,3,4,1))
   val resu: RDD[(Int, Iterable[String])] = value.map((_,"爱")).groupByKey()
    val unit: RDD[String] = resu.flatMap {
      case (item, itera) =>
        itera.filter(item => item.contains(item))

    }
    unit.foreach(println(_))

    }



////    val result: RDD[String] = infos.flatMap(one => {
//      one.split(" ")
//    })
//    result
//    result.foreach(println)
//  }
}
