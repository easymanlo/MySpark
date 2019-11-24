package com.bjsxt.scalaspark.sparkMlib.examples

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.sql.SparkSession

object OneHotEncoderExample {


  def main(args: Array[String]): Unit = {
//    val sparkConf = new SparkConf()
    val spark = SparkSession
      .builder
      .appName("OneHotEncoderExample")
//      .master("local")
      .getOrCreate()


    // $example on$
    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)
    val indexed = indexer.transform(df)

    val encoder = new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")

    val encoded = encoder.transform(indexed)
    encoded.printSchema()
    encoded.show()
    // $example off$

    spark.stop()
  }


}
