package com.bjsxt.scalaspark.Test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class tbStock(ordernumber: String, locationid: String, dateid: String) extends Serializable

case class tbStockDetail(ordernumber: String, rownum: Int, itemid: String, number: Int, price: Double, amount: Double) extends Serializable

case class tbDate(dateid: String, years: Int, theyear: Int, month: Int, day: Int, weekday: Int, week: Int, quarter: Int, period: Int, halfmonth: Int) extends Serializable

object hiveTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("createdataframefrommysql")
//      这个默认是200个任务小的可以设置少一点
      .config("spark.sql.shuffle.partitions",20)
      .getOrCreate()
    import spark.implicits._
    val tbstock: RDD[String] = spark.sparkContext.textFile("./data/tbStock.txt")
    val tbStockDs: Dataset[tbStock] = tbstock.map(_.split(",")).map(attr => tbStock(attr(0), attr(1), attr(2))).toDS()
//    tbStockDs.show(10)
    val tbdate: RDD[String] = spark.sparkContext.textFile("./data/tbDate.txt")
    val tbdateDs: Dataset[tbDate] = tbdate.map(_.split(",")).map(attre => tbDate(attre(0), attre(1).trim().toInt, attre(2).trim().toInt, attre(3).trim().toInt, attre(4).trim().toInt, attre(5).trim().toInt, attre(6).trim().toInt, attre(7).trim().toInt, attre(8).trim().toInt, attre(9).trim().toInt)).toDS()
//    tbdateDs.show(10)
    val tbstockdetail: RDD[String] = spark.sparkContext.textFile("./data/tbStockDetail.txt")
    val tbstockdetailDs: Dataset[tbStockDetail] = tbstockdetail.map(_.split(",")).map(attree => tbStockDetail(attree(0), attree(1).trim().toInt, attree(2), attree(3).trim().toInt, attree(4).trim().toDouble, attree(5).trim().toDouble)).toDS()
//    tbstockdetailDs.show(10)
    tbdateDs.createOrReplaceTempView("tbDate")
    tbStockDs.createOrReplaceTempView("tbStock")
    tbstockdetailDs.createOrReplaceTempView("tbStockDetail")
    val df: DataFrame = spark.sql("select c.theyear,COUNT(DISTINCT a.ordernumber),SUM(b.amount)" +
      "FROM" +
      " tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber" +
      " JOIN tbDate c ON a.dateid = c.dateid " +
      "GROUP BY c.theyear " +
      " ORDER BY c.theyear")
    df.show()


  }


}
