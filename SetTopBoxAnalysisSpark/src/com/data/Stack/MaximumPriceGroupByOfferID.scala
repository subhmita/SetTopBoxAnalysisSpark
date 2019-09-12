package com.data.Stack

import org.apache.spark.sql.SparkSession
import java.lang.Long
import java.text.SimpleDateFormat
import java.util.Date
import scala.xml.XML
import java.util.Calendar
import java.lang.Float

object MaximumPriceGroupByOfferID {

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
          evId == 102 ||evId == 103
        }
    }
       .map {
          line => {
            val tokens = line.split("\\^")
            val evId = Integer.parseInt(tokens(2).toString())
            var price_value = 0F
            var offer_id_value = ""
            val body = tokens(4).toString()
            val dvId = tokens(5).toString()
            val xml = XML.loadString(body)
            for (nv <- xml.child) {
              //println("Rows :"+nv)
              val oneNV = XML.loadString(nv.toString())
              val oneNVName = oneNV.attribute("n").fold("")(_.toString)
              val oneNVValue = oneNV.attribute("v").fold("")(_.toString)
              //println("oneNVName :"+oneNVName)
              if(oneNVName == "Price" && oneNVValue!="") {
                price_value = Float.parseFloat(oneNVValue)
              }
              if(oneNVName == "OfferId") {
                offer_id_value = oneNVValue
              }
            }
            (offer_id_value, price_value)
         }
      }
      .groupByKey()
      .map( rec => (rec._1, rec._2.max))
     // .reduceByKey(_+_)
      .sortBy(rec => (rec._2), false)
      //.take(5)
      result.foreach(println)
    //  println("Total number of devices : " + result.count())
      
      spark.stop
  }
}