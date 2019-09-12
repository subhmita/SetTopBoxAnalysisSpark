package com.data.Stack

import org.apache.spark.sql.SparkSession
import java.lang.Long
import java.text.SimpleDateFormat
import java.util.Date
import scala.xml.XML
import java.util.Calendar

object JunkRecords {

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
          evId == 0
        }
    }.filter { 
        line => {
          val tokens = line.split("\\^")
          val body = tokens(4).toString()
          body.contains("<d>")
        }
      }
      .map {
          line => {
            val tokens = line.split("\\^")
            var badblocks = ""
            val body = tokens(4).toString()
            val dvId = tokens(5).toString()
            //println("body :"+body)
            val xml = XML.loadString(body)
            for (nv <- xml.child) {
              //println("Rows :"+nv)
              val oneNV = XML.loadString(nv.toString())
              val oneNVName = oneNV.attribute("n").fold("")(_.toString)
              val oneNVValue = oneNV.attribute("v").fold("")(_.toString)
              //println("oneNVValue :"+oneNVValue)
              if(oneNVName == "BadBlocks") {
                badblocks = oneNVName
              }
            }
            //println("badblocks :"+line+" : "+badblocks)
            (badblocks, line)
         }
      }
      .filter(x => (x._1 =="BadBlocks"))
      //.take(5)
      
      result.foreach(println)
      println("Total junk records are thier having BadBlocks in xml column : " + result.count())
            spark.stop
  }
}