package classes

import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}

object MainAle {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkProject")

    val sc = new SparkContext(sparkConf)

    val sqlContext = new HiveContext(sc)
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    import spark.sqlContext.implicits._
    val schema = ScalaReflection.schemaFor[Finale].dataType.asInstanceOf[StructType]
    val properties = loadProperties()
    val dfJson = sqlContext.read.schema(schema).json(System.getProperty("user.dir") + "\\src\\main\\resources\\file.json")
    dfJson.dtypes.foreach(println)
    val datasetParsed = dfJson.as[FinaleForRDD]


    //DATAFRAME//
    //numero di commits
     val nCommits = println(dfJson.select($"payload.commits").count())
    //numero di commits per ogni actor
     val numeroCAttori = dfJson.withColumn("actor", $"actor".as("actor")).groupBy($"actor").count().show(24)
    //numero di commit divisi per type e actor
     val numeroCommitAT = dfJson.select($"Actor".as("actor"), $"type".as("type")).groupBy($"actor", $"type").count().show(24)
    //numero di commit divisi per type e actor e event
     dfJson.createOrReplaceTempView("tabella")
     sqlContext.sql("select Type , count(Type),Actor  from tabella   group by Type,Actor ").show(10)
    //numero di commit divisi per type e actor e ora
    val numeroCommitATO = dfJson.groupBy($"actor", $"type",$"Created_at").agg($"Type").count()
    println(numeroCommitATO)
    //max/min commit per ora
    val numeroCommitOMin = dfJson.groupBy($"payload.commits",$"Created_at").agg(count($"Created_at") as("e")).agg(min("e")).show()
    val numeroCommitOMax = dfJson.groupBy($"payload.commits",$"Created_at").agg(count($"Created_at") as("a")).agg(max("a")).show()

    //max/min commit per actor
    val numeroCommitAMin = dfJson.groupBy($"payload.commits",$"actor").agg(count($"payload.commits") as("e")).agg(min("e")).show()
    val numeroCommitAMax = dfJson.groupBy($"payload.commits",$"actor").agg(count($"payload.commits") as("a")).agg(max("a")).show()
    //max/min commit per repo
    val numeroCommitRMin = dfJson.groupBy($"payload.commits",$"repo").agg(count($"payload.commits") as("e")).agg(min("e")).show()
    val numeroCommitRMax = dfJson.groupBy($"payload.commits",$"repo").agg(count($"payload.commits") as("a")).agg(max("a")).show()
    //max/min commit per ora e actor
    val numeroCommitOAMin = dfJson.groupBy($"payload.commits",$"Created_at",$"actor").agg(count($"payload.commits") as("e")).agg(min("e")).show()
    val numeroCommitOMAax = dfJson.groupBy($"payload.commits",$"Created_at",$"actor").agg(count($"payload.commits") as("a")).agg(max("a")).show()
    //max/min commit per ora e repo
    val numeroCommitORMin = dfJson.groupBy($"payload.commits",$"Created_at",$"repo").agg(count($"payload.commits") as("e")).agg(min("e")).show()
    val numeroCommitORAax = dfJson.groupBy($"payload.commits",$"Created_at",$"repo").agg(count($"payload.commits") as("a")).agg(max("a")).show()
    //max/min commit per repo e actor
    val numeroCommitRAMin = dfJson.groupBy($"payload.commits",$"repo",$"actor").agg(count($"payload.commits") as("e")).agg(min("e")).show()
    val numeroCommitRAMax = dfJson.groupBy($"payload.commits",$"repo",$"actor").agg(count($"payload.commits") as("a")).agg(max("a")).show()



    //DATASET//
    //numero di commits
    val queryDS = datasetParsed.select($"payload.commits").count()
    println(queryDS)
    //numero di commits per ogni actor
    val nCommA = datasetParsed.withColumn("actor", $"actor".as("actor")).groupBy($"actor").count().show(24)
    //numero di commit divisi per type e actor
    val nCommAT = datasetParsed.select($"Actor".as("actor"), $"type".as("type")).groupBy($"actor", $"type").count().show(24)
    //numero di commit divisi per type e actor e event
     //-----------------------------------------------------
    //numero commit divisi per type e actor e ora
    val nCommTAO = datasetParsed.groupBy($"actor", $"type",$"Created_at").agg($"Type").count()
    println(nCommTAO)
    //max/min commit per ora
    val nCommMin = datasetParsed.groupBy($"payload.commits",$"Created_at").agg(count($"payload.commits") as("ab")).agg(min("ab")).show()
    val nCommMax = datasetParsed.groupBy($"payload.commits",$"Created_at").agg(count($"payload.commits") as("a")).agg(max("a")).show()
    //max/min commit per actor
    val nCommAMin = datasetParsed.groupBy($"payload.commits",$"actor").agg(count($"payload.commits") as("e")).agg(min("e")).show()
    val nCommAMax = datasetParsed.groupBy($"payload.commits",$"actor").agg(count($"payload.commits") as("a")).agg(max("a")).show()
    //max/min commit per repo
    val nCommRMin = datasetParsed.groupBy($"payload.commits",$"repo").agg(count($"payload.commits") as("e")).agg(min("e")).show()
    val nCommRMax = datasetParsed.groupBy($"payload.commits",$"repo").agg(count($"payload.commits") as("a")).agg(max("a")).show()
    //max/min commit per ora e actor
    val nCommOAMin = datasetParsed.groupBy($"payload.commits",$"Created_at",$"actor").agg(count($"payload.commits") as("e")).agg(min("e")).show()
    val nCommOMAax = datasetParsed.groupBy($"payload.commits",$"Created_at",$"actor").agg(count($"payload.commits") as("a")).agg(max("a")).show()
    //max/min commit per ora e repo
    val nCommORMin = datasetParsed.groupBy($"payload.commits",$"Created_at",$"repo").agg(count($"payload.commits") as("e")).agg(min("e")).show()
    val nCommORMax = datasetParsed.groupBy($"payload.commits",$"Created_at",$"repo").agg(count($"payload.commits") as("a")).agg(max("a")).show()
    //max/min commit per repo e actor
    val nCommRAMin = datasetParsed.groupBy($"payload.commits",$"repo",$"actor").agg(count($"payload.commits") as("e")).agg(min("e")).show()
    val nCommRAMax = datasetParsed.groupBy($"payload.commits",$"repo",$"actor").agg(count($"payload.commits") as("a")).agg(max("a")).show()

  }
  def loadProperties(): Properties = {
    val prop = new Properties()
    prop.load(new FileInputStream(new File(System.getProperty("user.dir") + "\\src\\main\\resources\\application.properties")))
    prop
  }
}
