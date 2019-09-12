package com.data.Stack

import org.apache.spark.sql.SparkSession
import java.lang.Long
import java.text.SimpleDateFormat
import java.util.Date
import scala.xml.XML
import java.util.Calendar

object GroupAllButtonWithDvId {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: TopFiveDevices <Input-File> <Output-File>");
      System.exit(1);
    }
    val spark = SparkSession
      .builder
      .appName("TotalDeviceswithPowerMode")
      .getOrCreate()

    val data = spark.read.textFile(args(0)).rdd

    val result = data.filter {
      line =>
        {
          val tokens = line.split("\\^")
          val evId = Integer.parseInt(tokens(2).toString())
            evId == 107 
         }
      }
      .map {
          line => {
            val tokens = line.split("\\^")
            var button_name_value = ""
            val body = tokens(4).toString()
            val dvId = tokens(5).toString()
            val xml = XML.loadString(body)
            for (nv <- xml.child) {
              //println("Rows :"+nv)
              val oneNV = XML.loadString(nv.toString())
              val oneNVName = oneNV.attribute("n").fold("")(_.toString)
              val oneNVValue = oneNV.attribute("v").fold("")(_.toString)
              //println("oneNVValue :"+oneNVValue)
              if(oneNVName == "ButtonName" && oneNVValue!="") {
                button_name_value = oneNVValue
              }
            }
            (button_name_value, dvId)
         }
      }
      .groupByKey()
      .sortByKey(false)
      //.take(5)
      result.foreach(println)
      spark.stop
  }
}