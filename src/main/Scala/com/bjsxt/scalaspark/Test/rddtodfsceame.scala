package com.bjsxt.scalaspark.Test
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object rddtoDS1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").appName("tt").getOrCreate()
    val rdd1: RDD[String] = spark.sparkContext.textFile("./data/peop.txt")
//    这个是动态生成的
    val scheamString="name age"
   /* val fileds: Array[StructField] = scheamString.split(" ")
      .map(filedsName=>StructField(filedsName,StringType,nullable = true))*/

    val fields: Array[StructField] = scheamString.split(" ").map(filedsName => filedsName match {
      case "name" => StructField(filedsName, StringType, nullable = true);
      case "age" => StructField(filedsName, IntegerType, nullable = true)
    })

      //StructField  相当于每一列，列名的类型， StructType相当于row
    val scheam =StructType(fields)
//    trim这个函数是去掉空格
    import org.apache.spark.sql._
    val rdd2: RDD[Row] = rdd1.map(_.split(",")).map(attribute =>
      Row(attribute(0).trim, attribute(1).trim))
    val df: DataFrame = spark.createDataFrame(rdd2,scheam)
    df.show()

    
  }

}
