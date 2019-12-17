import java.awt.Desktop

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object MainIsaac {
 def main(args: Array[String]) {
    println("Hello,")
    val conf = new SparkConf().setMaster("local[2]")
      .setAppName("CountingSheep")

    val sc = new SparkContext(conf)

    val sqlContext = new HiveContext(sc)
   import sqlContext.implicits._
   // val jsonDF = sqlContext.read.json("C:\\Users\\quiaz\\Desktop\\file json")
    //jsonDF.show

 //  jsonDF.dtypes.foreach(println)
   /*val dFjson = sqlContext.read.json("C:\\Users\\quiaz\\Desktop\\Org\\org.json")
   dFjson.dtypes.foreach(println)
   val rdd = dFjson.as[Org].rdd
   rdd.foreach(x => println(x.toString))*/

   val jsonDf = sqlContext.read.json("C:\\Users\\quiaz\\Desktop\\TestSpark\\Repo")
   jsonDf.dtypes.foreach(println)
   val rdd = jsonDf.as[Repo].rdd
   rdd.foreach(x => println(x.toString))


  }
}
