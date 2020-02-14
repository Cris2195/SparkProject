package com.progetto


import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object MainAle {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkProject")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val properties = loadProperties()
    val dfJson = sqlContext.read.json(System.getProperty("user.dir") + "\\src\\main\\resources\\file.json")
    dfJson.dtypes.foreach(println)

    //numero di commits
    val nCommits = println(dfJson.select($"payload.commits").count())
    //numero di commits per ogni actor
    val nCommitsActor = dfJson.withColumn("author", explode($"payload.commits.author")).groupBy($"payload.commits.author").count().show(10)
    //numero di commit divisi per type e actor
    val x1= dfJson.select($"payload.commits.author").as("Author").groupBy("author").count().show(10)
    val x2= dfJson.select($"payload.member.type").as("Type").groupBy("type").count().show(10)
    //numero di commit divisi per type e actor e event
    //  dfJson.select($"Type",$"CommitPrincipale").groupBy($"Type",$"Actor",$"Event").count().show(20)
  }

  def loadProperties(): Properties = {
    val prop = new Properties()
    prop.load(new FileInputStream(new File(System.getProperty("user.dir") + "\\src\\main\\resources\\application.properties")))
    prop
  }
}