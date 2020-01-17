import java.io.{BufferedInputStream, BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.net.{HttpURLConnection, URL}
import java.util.Properties
import java.util.zip.GZIPInputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
object MainCristian {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkProject")

    val context = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(context)
    context.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    context.hadoopConfiguration.set("parquet.enable.summary-metadata", "false")
    import sqlContext.implicits._
    val properties = loadProperties()
  downloadFile("http://data.githubarchive.org/"+properties.getProperty(EnumString.anno.toString)+"-"+properties.getProperty(EnumString.mese.toString)+
  "-"+properties.getProperty(EnumString.giorno.toString)+"-0"+".json.gz")
    val dfJson = sqlContext.read.json(System.getProperty("user.dir")+"\\src\\main\\resources\\File.json")

   dfJson.dtypes.foreach(println)

    //Distinct actor su file csv
    val dataFrameSoloActor = dfJson.select($"Actor").distinct()
    if(! new File(System.getProperty("user.dir")+"\\src\\main\\resources\\actor").exists())
            dataFrameSoloActor.coalesce(1).write.format("com.databricks.spark.csv").save(System.getProperty("user.dir")+"\\src\\main\\resources\\actor")

  //singoli author per commit su csv
    val dfSoloAuthor=dfJson.select($"Payload.commits.author").distinct()
    if(! new File(System.getProperty("user.dir")+"\\src\\main\\resources\\authorPerCommit").exists())
          dfSoloAuthor.coalesce(1).write.format("com.databricks.spark.csv").save(System.getProperty("user.dir")+"\\src\\main\\resources\\authorPerCommit")


    val dfSoloRepo = dfJson.select($"Repo").distinct()
    if(! new File(System.getProperty("user.dir")+"\\src\\main\\resources\\repoSingoli").exists())
        dfSoloRepo.coalesce(1).write.format("com.databricks.spark.csv").save(System.getProperty("user.dir")+"\\src\\main\\resources\\repoSingoli")


    val dfTipiEvento = dfJson.select($"Type").distinct()
    if(! new File(System.getProperty("user.dir")+"\\src\\main\\resources\\eventi").exists())
        dfTipiEvento.coalesce(1).write.format("com.databricks.spark.csv").save(System.getProperty("user.dir")+"\\src\\main\\resources\\eventi")


    //Numero di actor
    println(dfJson.select($"Actor").count())
    //Numero di repo
    println(dfJson.select(dfJson.col("Repo")).count())

    //Numero di event per ogni actor e type
    dfJson.registerTempTable("tabella")
    sqlContext.sql("select Type , count(Type),Actor  from tabella   group by Type,Actor ").show()








  }

  def loadProperties(): Properties ={
    val prop = new Properties()
    prop.load(new FileInputStream(new File(System.getProperty("user.dir")+"\\src\\main\\resources\\application.properties")))
    prop
  }
  def downloadFile(url : String): Unit ={
    val realUrl : URL = new URL(url)
    val conn :HttpURLConnection = realUrl.openConnection().asInstanceOf[HttpURLConnection]
    val outputStream = new BufferedOutputStream(new FileOutputStream(new File(System.getProperty("user.dir")+"\\src\\main\\resources\\File.json")))
    conn.connect()
    conn.setInstanceFollowRedirects(true)
    var redirect : Boolean = false
    if(conn.getResponseCode == 301 ){
      redirect = true
    }
    if (redirect){
      val newConn = new URL(conn.getHeaderField("Location")).openConnection().asInstanceOf[HttpURLConnection]
      newConn.addRequestProperty("User-Agent", "")
      newConn.connect()
      val reader =new GZIPInputStream((new BufferedInputStream( newConn.getInputStream)))

      try{
        IOUtils.copy(reader,outputStream)
        IOUtils.closeQuietly(reader)
        IOUtils.closeQuietly(outputStream)
      }
      catch{
        case x:Exception =>{
          println("Something went wrong while unzipping the file" + x.getMessage)
        } }
      finally{
        outputStream.flush()
        newConn.disconnect()
        outputStream.close()
        reader.close()

      }
    }
  }
}

