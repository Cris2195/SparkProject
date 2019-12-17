



import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import collection.mutable.Stack
import org.scalatest._

class TraitsTest extends FlatSpec with Matchers {

  "Actor dataframe" should "convert correctly with Actor case class" in {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkProject")

    val context = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(context)
    import sqlContext.implicits._

    val dfJson = sqlContext.read.json("C:\\Users\\Studente\\Desktop\\actor.json")
    // dfJson.show()
    dfJson.dtypes.foreach(println)
    val rdd = dfJson.as[Actor].rdd

    println(rdd.count())


    assert(true)
  }

}