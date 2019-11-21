package com.bjsxt.scalaspark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object TransformBlackList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("transform")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf,Durations.seconds(5))
//    ssc.sparkContext.setLogLevel("Error")
    /**
      * 广播黑名单
      */
    val blackList: Broadcast[List[String]] = ssc.sparkContext.broadcast(List[String]("zhangsan","lisi"))

    /**
      * 从实时数据【"hello zhangsan","hello lisi"】中发现 数据的第二位是黑名单人员，过滤掉
      */
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node009",9999)
//    hello lisi  这个字符串就是截的是李四  然后  map是 key  value   lisi   1
    val pairLines: DStream[(String, String)] = lines.map(line=>{(line.split(" ")(1),line)})
//    transform算子可以拿到DStream中的RDD，对RDD使用的算子操作，但是最后要返回RDD，返回的RDD又被封装到一个DStream
//    (pairRDD:RDD[(String,String)]   tranform拿到key value 格式的RDD，里面是一个touple
    /*
    * transform中拿到的RDD的算子外，代码是在Drive端执行的。可以做到动态的改变广播变量
    * */
    val resultDStream: DStream[String] = pairLines.transform((pairRDD:RDD[(String,String)]) => {
      val filterRDD: RDD[(String, String)] = pairRDD.filter(tp => {
        val nameList: List[String] = blackList.value
//        tp_1 就是指的是zhangsan lisi 不包含的都留下
        !nameList.contains(tp._1)
      })
      val returnRDD: RDD[String] = filterRDD.map(tp => tp._2)
      returnRDD
    })

    resultDStream.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }
}
