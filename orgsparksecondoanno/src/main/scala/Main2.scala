import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
object Main2 {
    def main(args: Array[String]) {

      val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkProject")

      val context = new SparkContext(sparkConf)
      val sqlContext = new HiveContext(context)
      import sqlContext.implicits._

      val dfJson = sqlContext.read.json("C:\\Users\\Studente\\Desktop\\filee.json")



    }
  }
