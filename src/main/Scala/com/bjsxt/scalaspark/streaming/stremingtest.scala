package com.bjsxt.scalaspark.streaming

import java.time.Duration

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object stremingtest {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setAppName("stremingtest")
    conf.setMaster("local[2]")
//    val sc = new SparkContext(conf)
    //bitch
      val ssc = new StreamingContext(conf,Durations.seconds(5))
//    监控node009的9999端口
    ssc.sparkContext.setLogLevel("ERROR")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("node009",9999)
     val words: DStream[String] = lines.flatMap(one=>{one.split(" ")})
     val pairwords: DStream[(String, Int)] = words.map(one=>{(one,1)})
    val result: DStream[(String, Int)] = pairwords.reduceByKey((v1:Int,v2:Int)=>{v1+v2})
    result.print(100)
    ssc.start()
//    每隔五秒执行
    ssc.awaitTermination()
    ssc.stop()

  }

}
