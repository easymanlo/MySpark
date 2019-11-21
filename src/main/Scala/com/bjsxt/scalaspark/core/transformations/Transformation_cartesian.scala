package com.bjsxt.scalaspark.core.transformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Transformation_cartesian {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("cartesian").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val left  = sc.parallelize(List(1,2,4,5))
    val right  = sc.parallelize(List(3,4,5,6))
    //(1,3)(1,4)(1,5)(1,6)(2,3)(2,4)(2,5)(2,6)(3,3)(3,4)(3,5)(3,6)
    val result: RDD[(Int, Int)] = left.cartesian(right)
    result.foreach(print)

  }



}
