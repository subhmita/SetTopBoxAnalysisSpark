package com.data.Stack

import org.apache.spark.sql.SparkSession
import java.lang.Long
import java.text.SimpleDateFormat
import java.util.Date
import scala.xml.XML
import java.util.Calendar

object TopFiveDevices {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: TopFiveDevices <Input-File> <Output-File>");
      System.exit(1);
    }
    val spark = SparkSession
      .builder
      .appName("TopFiveDevices")
      .getOrCreate()

    val data = spark.read.textFile(args(0)).rdd

    val result = data.filter {
      line =>
        {
          val tokens = line.split("\\^")
          val evId = Integer.parseInt(tokens(2).toString())
          evId == 100
        }
    }
      .map {
        line =>
          {
            val tokens = line.split("\\^")
            val evId = Integer.parseInt(tokens(2).toString())
            var duration_value = 0
            val body = tokens(4).toString()
            val dvId = tokens(5).toString()
            val xml = XML.loadString(body)
            for (nv <- xml.child) {
              println("Rows :" + nv)
              val oneNV = XML.loadString(nv.toString())
              println("oneNV" + oneNV)
              val oneNVName = oneNV.attribute("n").fold("")(_.toString)
              val oneNVValue = oneNV.attribute("v")
              println("oneNVName :" + oneNVName)
              if (oneNVName == "Duration") {
                duration_value = Integer.parseInt(oneNVValue.getOrElse(0).toString())
              }
            }
            (dvId, duration_value)
          }
      }
      .groupByKey()
      .map(rec => (rec._1, rec._2.max))
      .sortBy(rec => (rec._2), false)
      .take(5)

    result.foreach(println)
    spark.stop
  }
}