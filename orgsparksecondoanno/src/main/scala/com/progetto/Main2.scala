package com.progetto

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Main2 {
    def main(args: Array[String]) {

      val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkProject")

      val context = new SparkContext(sparkConf)
      val sqlContext = new HiveContext(context)

      val dfJson = sqlContext.read.json("C:\\Users\\Studente\\Desktop\\filee.json")



    }
  }
