package com.bjsxt.scalaspark.Test

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

class CustomRecvicer(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  override def onStart(): Unit = {
    new Thread("Socket Receiver") {

      //      alt+insertk快件键覆写这个类的方法
      override def run() {
        receiver()
      }
    }.start()


  }

  override def onStop(): Unit = {
  }

  def receiver() = {
    var socket: Socket = null
    var userinput: String = null
    socket = new Socket(host, port)
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
    userinput = reader.readLine()
    while (!isStopped() && userinput != null) {
      store(userinput)
      userinput = reader.readLine()
    }
    reader.close()
    socket.close()
  }
}

  object CustomRecvicer {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
      conf.setAppName("socket")
      conf.setMaster("local[2]")
//      val sc = new SparkContext(conf)
      val ssc = new StreamingContext(conf, Seconds(5))
       ssc.sparkContext.setLogLevel("ERROR")
      val line: ReceiverInputDStream[String] = ssc.receiverStream(new CustomRecvicer("node009", 9999))
      val words: DStream[String] = line.flatMap(_.split(" "))
      val pairs: DStream[(String, Int)] = words.map((_, 1))
      val result: DStream[(String, Int)] = pairs.reduceByKey((_ + _))
      result.print()
      ssc.start()
      ssc.awaitTermination()


    }
  }






