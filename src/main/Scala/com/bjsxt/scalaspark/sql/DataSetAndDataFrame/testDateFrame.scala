package com.bjsxt.scalaspark.sql.DataSetAndDataFrame

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object testDateFrame {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("testDateFrame").master("local").getOrCreate()
    val jsonList = List[String](
      "{\"name\":\"zhangsan\",\"age\":20}",
      "{\"name\":\"lisi\",\"age\":21}",
      "{\"name\":\"wangwu\",\"age\":22}"


    )
//       val df: DataFrame = spark.read.format("json").load("./data/json")
    import spark.implicits._
    val jsonDs: Dataset[String] = jsonList.toDS()
    val df: DataFrame = spark.read.json(jsonDs)
    df.map{
      case Row(col1:Int,col2:String)=>
        println(col1);println(col2)
        col2

      case _=>
        ""
    }

  }

}
