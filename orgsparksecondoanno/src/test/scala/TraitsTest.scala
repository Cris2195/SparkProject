



import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import collection.mutable.Stack
import org.scalatest._

/*class TraitsTest extends FlatSpec with Matchers {

  "com.progetto.Actor dataframe" should "convert correctly with com.progetto.Actor case class" in {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkProject")

    val context = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(context)
    import sqlContext.implicits._

    val dfJson = sqlContext.read.json("C:\\Users\\Studente\\Desktop\\actor.json")
    // dfJson.show()
    dfJson.dtypes.foreach(println)
    val rdd = dfJson.as[com.progetto.Actor].rdd

    println(rdd.count())


    assert(true)
  }
  "com.progetto.Org dataframe" should "convert correctly with org case class" in {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkProject")

    val context = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(context)
    import sqlContext.implicits._

    val dFjson = sqlContext.read.json("C:\\Users\\quiaz\\Desktop\\com.progetto.Org\\org.json")
    dFjson.dtypes.foreach(println)
    val rdd = dFjson.as[com.progetto.Org].rdd

    println(rdd.count())
    assert(true)

  }
  "com.progetto.Repo dataframe" should "convert correctly with repo case class" in {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkProject")

    val context = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(context)
    import sqlContext.implicits._

    val dFjson = sqlContext.read.json("C:\\Users\\quiaz\\Desktop\\TestSpark\\com.progetto.Repo")
    dFjson.dtypes.foreach(println)
    val rdd = dFjson.as[com.progetto.Repo].rdd

    println(rdd.count())
    assert(true)

  }


}*/