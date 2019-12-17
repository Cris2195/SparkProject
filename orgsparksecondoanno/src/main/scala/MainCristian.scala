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

    val dfJson = sqlContext.read.json("C:\\Users\\Studente\\Desktop\\actor.json")
   // dfJson.show()
   dfJson.dtypes.foreach(println)
    val rdd = dfJson.as[Actor].rdd

    rdd.foreach(println)





  }
}

