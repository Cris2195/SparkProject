package com.progetto

import java.io._
import java.net.{HttpURLConnection, URL}
import java.nio.file.{Files, Paths}
import java.util.Properties
import java.util.zip.GZIPInputStream

import com.opencsv.CSVWriter
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.ClassTag

object MainCristian {
  def main(args: Array[String]) {
    val sparkProperties = loadSparkProperties()
    val sparkConf = new SparkConf().setMaster(sparkProperties.getProperty("master"))
      .setAppName("SparkProject").
      set("spark.debug.maxToStringFields", "1000")

    val spark = SparkSession
      .builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    val properties = loadApplicationProperties()
    // downloadFile("http://data.githubarchive.org/"+properties.getProperty(EnumString.anno.toString)+"-"+properties.getProperty(EnumString.mese.toString)+
    //  "-"+properties.getProperty(EnumString.giorno.toString)+"-0"+".json.gz")

    val schema = ScalaReflection.schemaFor[WrapperForStruct].dataType.asInstanceOf[StructType]
    val dfJson = spark.read
      .schema(schema)
      .json(System.getProperty("user.dir") + "\\src\\main\\resources\\File.json")

    val rdd = dfJson.as[Wrapper].rdd
    val dataSet = dfJson.as[Wrapper]

    // distinct actor su file csv
    val dataFrameSoloActor = dfJson.select($"actor.*").distinct()
    saveDataFrameOnCsvFile(dataFrameSoloActor, "actor")
    val dataSetActor = dataSet.select($"actor.*").as[Actor].distinct()
    saveDataSetOnCsvFile(dataSetActor, "actorDataframe")
    val rddActor = rdd.filter(x => x.actor != null).map(x => x.actor).distinct()
    saveRddOnCsvFile(rddActor, "actorRdd")

    //singoli author per commit su csv
    val dfSoloAuthor = dfJson.select($"payload.commits.author.*").distinct()
    saveDataFrameOnCsvFile(dfSoloAuthor, "authorDataFrame")
    val dataSetAuthor = dataSet.filter(x => x.payload != null && x.payload.commits != null && x.payload.commits.author != null).map(x => x.payload.commits.author).distinct()
    saveDataSetOnCsvFile(dataSetAuthor, "authorDataset")
    val rddAuthor = rdd.filter(x => x.payload != null && x.payload.commits != null && x.payload.commits.author != null
    ).map(x => x.payload.commits.author).groupBy(author => author.email)
    saveRddOnCsvFile(rddAuthor, "authorRdd")


    //singoli repo
    val dfSoloRepo = dfJson.select($"repo").distinct()
    saveDataFrameOnCsvFile(dfSoloRepo, "repoDataFrame")
    val dataSetRepoFull = dfJson.as[Wrapper]
    val dataSetRepo = dataSet.select(dataSetRepoFull.col("repo")).distinct()
    saveDataSetOnCsvFile(dataSetRepo, "repoDataset")
    val repoRdd = rdd.filter(x => x.repo != null).map(x => x.repo).distinct()
    saveRddOnCsvFile(repoRdd, "repoRdd")


    //tipi di evento
    val eventType = dfJson.select($"type").distinct()
    saveDataFrameOnCsvFile(eventType, "eventTypeDataFrame")
    val eventTypeDataset = dataSet.select($"type").distinct()
    saveDataSetOnCsvFile(eventTypeDataset, "eventtypeDataset")
    val rddEvent = rdd.filter(x => x.`type` != null).map(x => x.`type`).distinct()
    saveRddOnCsvFile(rddEvent, "eventTypeRdd")


    //Numero di actor
    dfJson.select($"actor").count()
    val numeroActor =  rdd.map(x=> x.actor).count()
    dataSet.select($"actor").count()


    //Numero di repo
    dfJson.select(dfJson.col("repo")).count()
    rdd.map(x => x.repo).count()
    dataSet.select($"repo").count()

    //numero di event per ogni actor
    dfJson.select($"Type", $"Actor").groupBy($"Actor").count().show(10)
    rdd.map(x=> (x.actor,x.`type`)).groupByKey().map(x => {(x._1,x._2.size)})
    dataSet.map(x => (x.actor,x.`type`)).groupBy($"actor")

    //numero di event per ogni actor e type
    dfJson.select($"Type", $"Actor").groupBy($"Actor", $"Type").count().show(12)




    //Numero di event per ogni actor e type e repo
    dfJson.createOrReplaceTempView("tabella")
     spark.sql("select Type , count(Type),Actor,Repo  from tabella   group by Type,Actor, Repo ")


  }

  def loadApplicationProperties(): Properties = {
    val prop = new Properties()
    prop.load(new FileInputStream(new File(System.getProperty("user.dir") + "\\src\\main\\resources\\application.properties")))
    prop
  }

  def loadSparkProperties(): Properties = {
    val prop = new Properties()
    prop.load(new FileInputStream(new File(System.getProperty("user.dir") + "\\src\\main\\resources\\spark.properties")))
    prop
  }


  def saveDataFrameOnCsvFile(datasource: DataFrame, path: String): Unit = {
    val writer = Files.newBufferedWriter(Paths.get(System.getProperty("user.dir") + "\\src\\main\\resources\\" + path + ".csv"))
    val csvWriter: CSVWriter = new CSVWriter(writer,
      CSVWriter.DEFAULT_SEPARATOR,
      CSVWriter.NO_QUOTE_CHARACTER,
      CSVWriter.DEFAULT_ESCAPE_CHARACTER,
      CSVWriter.DEFAULT_LINE_END)
    val dataTosave = datasource.collect().map(x => x.toString())
    csvWriter.writeNext(dataTosave)
    csvWriter.close()

  }

  def saveRddOnCsvFile[T: ClassTag](datasource: RDD[T], path: String): Unit = {
    val writer = Files.newBufferedWriter(Paths.get(System.getProperty("user.dir") + "\\src\\main\\resources\\" + path + ".csv"))
    val csvWriter: CSVWriter = new CSVWriter(writer,
      CSVWriter.DEFAULT_SEPARATOR,
      CSVWriter.NO_QUOTE_CHARACTER,
      CSVWriter.DEFAULT_ESCAPE_CHARACTER,
      CSVWriter.DEFAULT_LINE_END)
    val dataTosave = datasource.collect()
    val data = dataTosave.map(x => x.toString())
    csvWriter.writeNext(data)
    csvWriter.close()

  }

  def saveDataSetOnCsvFile[T: ClassTag](datasource: Dataset[T], path: String): Unit = {
    val writer = Files.newBufferedWriter(Paths.get(System.getProperty("user.dir") + "\\src\\main\\resources\\" + path + ".csv"))
    val csvWriter: CSVWriter = new CSVWriter(writer,
      CSVWriter.DEFAULT_SEPARATOR,
      CSVWriter.NO_QUOTE_CHARACTER,
      CSVWriter.DEFAULT_ESCAPE_CHARACTER,
      CSVWriter.DEFAULT_LINE_END)
    val dataTosave = datasource.collect().map(x => x.toString())
    csvWriter.writeNext(dataTosave)
    csvWriter.close()

  }

  def downloadFile(url: String): Unit = {
    val realUrl: URL = new URL(url)
    val conn: HttpURLConnection = realUrl.openConnection().asInstanceOf[HttpURLConnection]
    val outputStream = new BufferedOutputStream(new FileOutputStream(new File(System.getProperty("user.dir") + "\\src\\main\\resources\\File.json")))
    conn.connect()
    conn.setInstanceFollowRedirects(true)
    var redirect: Boolean = false
    if (conn.getResponseCode == 301) {
      redirect = true
    }
    if (redirect) {
      val newConn = new URL(conn.getHeaderField("Location")).openConnection().asInstanceOf[HttpURLConnection]
      newConn.addRequestProperty("User-Agent", "")
      newConn.connect()
      val reader = new GZIPInputStream((new BufferedInputStream(newConn.getInputStream)))

      try {
        IOUtils.copy(reader, outputStream)
        IOUtils.closeQuietly(reader)
        IOUtils.closeQuietly(outputStream)
      }
      catch {
        case x: Exception => {
          println("Something went wrong while unzipping the file" + x.getMessage)
        }
      }
      finally {
        outputStream.flush()
        newConn.disconnect()
        outputStream.close()
        reader.close()

      }
    }
  }

}
