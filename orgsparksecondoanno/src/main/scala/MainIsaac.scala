import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object MainIsaac {
 def main(args: Array[String]) {
    println("Hello,")
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("CountingSheep")

    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)
    val jsonDF = sqlContext.read.json("C:\\Users\\quiaz\\Desktop\\file json")
    //jsonDF.show

  }
}
