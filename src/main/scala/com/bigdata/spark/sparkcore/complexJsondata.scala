package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object complexJsondata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("complexJsondata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark._

    val data ="C:\\Bigdata\\datasets\\zips.json"
    val df =spark.read.format("json").load(data)
    df.createOrReplaceTempView("tab")
    df.printSchema()
    

    spark.stop()
  }
}