import java.io.{BufferedInputStream, BufferedOutputStream, File, FileOutputStream}
import java.net.{HttpURLConnection, URL}
import java.util.zip.GZIPInputStream

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
object MainCristian {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkProject")

    val context = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(context)
    import sqlContext.implicits._
downloadFile("http://data.githubarchive.org/2018-03-01-0.json.gz")
    val dfJson = sqlContext.read.json(System.getProperty("user.dir")+"\\src\\main\\resources\\File.json")

   dfJson.dtypes.foreach(println)







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

