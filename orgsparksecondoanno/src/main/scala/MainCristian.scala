import java.io._
import java.net.{HttpURLConnection, URL}
import java.util.Properties
import java.util.zip.GZIPInputStream

import com.opencsv.CSVWriter
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.{CoGroupedRDD, HadoopRDD, JdbcRDD, NewHadoopRDD, PartitionPruningRDD, ShuffledRDD, UnionRDD}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.ShuffledRowRDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import java.io.Writer
import java.nio.file.Files
import java.nio.file.Paths

import org.apache.spark.sql.catalyst.ScalaReflection
object MainCristian {
  def main(args: Array[String]) {
    val sparkProperties = loadSparkProperties()
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkProject").set("spark.debug.maxToStringFields","1000")

    val context = new SparkContext(sparkConf)

    val sqlContext = new HiveContext(context)
    val spark = SparkSession
      .builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    //context.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    //context.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
   // import sqlContext.implicits._
    val properties = loadApplicationProperties()
   // downloadFile("http://data.githubarchive.org/"+properties.getProperty(EnumString.anno.toString)+"-"+properties.getProperty(EnumString.mese.toString)+
  //  "-"+properties.getProperty(EnumString.giorno.toString)+"-0"+".json.gz")

    val schema = ScalaReflection.schemaFor[WrapperForStruct].dataType.asInstanceOf[StructType]
    val dfJson = sqlContext.read
        .schema(schema)
      .json(System.getProperty("user.dir") + "\\src\\main\\resources\\File.json")

dfJson.dtypes.foreach(println)
    //Distinct actor su file csv
    val dataFrameSoloActor = dfJson.distinct().as[Wrapper].rdd
    dataFrameSoloActor.collect().foreach(println)
   // dataFrameSoloActor.show()
  //saveOnCsvFile(dataFrameSoloActor,"actor")

    //singoli author per commit su csv
  /*  val dfSoloAuthor = dfJson.select($"Payload.commits.author")
   saveOnCsvFile(dfSoloAuthor,"author")


    val dfSoloRepo = dfJson.select($"Repo")





    val dfTipiEvento = dfJson.select($"Type").distinct().map(x => {x.toString()})
    if (!new File(System.getProperty("user.dir") + "\\src\\main\\resources\\eventi").exists())
      dfTipiEvento.coalesce(1).write.format("com.databricks.spark.csv").save(System.getProperty("user.dir") + "\\src\\main\\resources\\eventi")


    //Numero di actor
    println(dfJson.select($"Actor").count())
    //Numero di repo
    println(dfJson.select(dfJson.col("Repo")).count())

    //numero di event per ogni actor
    dfJson.select($"Type",$"Actor").groupBy($"Actor").count().show(10)
//numero di event per ogni actor e type
    dfJson.select($"Type",$"Actor").groupBy($"Actor",$"Type").count().show(12)

    //Numero di event per ogni actor e type e repo
    dfJson.createOrReplaceTempView("tabella")
    sqlContext.sql("select Type , count(Type),Actor,Repo  from tabella   group by Type,Actor, Repo ")

*/


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



  def saveOnCsvFile(datasource:DataFrame , path:String): Unit ={
    val writer = Files.newBufferedWriter(Paths.get(System.getProperty("user.dir")+"\\src\\main\\resources\\"+path+".csv"))
    val csvWriter : CSVWriter = new CSVWriter(writer,
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



