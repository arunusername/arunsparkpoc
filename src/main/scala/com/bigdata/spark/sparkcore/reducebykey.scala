package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object reducebykey {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("reducebykey").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    println("""arun""")
    import spark.implicits._
    import spark.sql
    val csvdata = "/C:/Bigdata/datasets-20201215T023939Z-001/datasets/asldata.txt"
    val csvrdd = sc.textFile(csvdata)
    val head = csvrdd.first()
    val res = csvrdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(2),1)).reduceByKey((x,y)=>x+y)
    res.foreach(println)
    spark.stop()
  }
}