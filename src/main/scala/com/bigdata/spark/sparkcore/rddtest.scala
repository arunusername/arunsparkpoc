package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object rddtest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rddtest").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    val data = "C:\\bigdata\\datasets\\bank-full.csv"
    // years back old strategy
    val rdd = sc.textFile(data)
    val head = rdd.first() // header// age; balance, marital,job
    val fields = head.split(";").map(x => StructField(x.replaceAll("\"", ""), StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD (people) to Rows
    val rowRDD = rdd.map(x => x.replaceAll("\"", "").split(";")).map(x => Row.fromSeq(x))

    // Apply the schema to the RDD
    val df = spark.createDataFrame(rowRDD, schema)
    df.createOrReplaceTempView("tab")
    df.show(5)
    df.printSchema()

    spark.stop()
  }
}