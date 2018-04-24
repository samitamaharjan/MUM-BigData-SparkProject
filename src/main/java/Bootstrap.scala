import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ DataFrame, SQLContext }
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.SaveMode
import scala.math.BigDecimal

case class Titanic(id: Int, gender: String, survive: Int, age: Double)

object Bootstrap extends App {
  override def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val csv = sc.textFile("/home/cloudera/workspace1/sparkproject/titanic_data/Titanic_data.csv")
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val dataRDD = headerAndRows.filter(_(0) != header(0))

    /* Alternative Method if there is only one file
    * val dataRDD = textFile.mapPartitionsWithIndex { (idex, iter) => if (idex == 0) iter.drop(1) else iter }
    */

    val filteredData = dataRDD.filter(x => !x(6).isEmpty())

    val requiredData = filteredData.map(x => Titanic(x(0).toInt, x(5), x(1).toInt, x(6).toDouble))

    // Initialize an SQLContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._
    import sqlContext.implicits._

    val dataDF = requiredData.toDF();
    dataDF.registerTempTable("titanic")
    val originalMean = sqlContext.sql("select gender, survive, cast(avg(age) as decimal(5,2)) avg_age, cast(stddev(age) as decimal(5,2)) std_dev_age from titanic group by gender, survive")
    // originalMean.collect().foreach(println)
    
    originalMean.repartition(1).write.mode(SaveMode.Overwrite).format("csv").save("/home/cloudera/workspace1/sparkproject/titanic_original_output")

    val sample = dataDF.sample(false, 0.25);
    
    var mapMean = HashMap.empty[String, ArrayBuffer[Tuple2[Double, Double]]]
    
    val n = 10;
    for (a <- 1 to n) {
      val sample1 = sample.sample(true, 1);
      sample1.registerTempTable("titanic1")
      val sampleMean = sqlContext.sql("select concat(gender, \"-\",  survive) as category, avg(age) avg_age, stddev(age) std_dev_age from titanic1 group by gender, survive")
      // sampleMean.show()
      val outputList = sampleMean.collectAsList()
      
      for (i <- 0 to outputList.size() - 1) {
        val key = outputList.get(i).get(0).asInstanceOf[String]
        val avgValue = outputList.get(i).get(1).asInstanceOf[Double]
        val sdValue = outputList.get(i).get(2).asInstanceOf[Double]
        
        var tuple = new Tuple2(avgValue, sdValue)
        if (mapMean.contains(key)) {
          var arr = mapMean.get(key).get
          arr += tuple
          mapMean(key) = arr
        } else {
          var newArr = ArrayBuffer[Tuple2[Double, Double]](tuple)
          mapMean(key) = newArr
        }
      }
      println("loop " + a + " of " + n)
    }
    val totalMeanAndSD = mapMean.map({case x => 
      var list = x._2.toList
      var sumOfMean = list.map(x => x._1).reduce(_+_)
      var sumOfSD = list.map(x => x._2).reduce(_+_)
      
      var totalAvg = BigDecimal(sumOfMean / list.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      var totalSD = BigDecimal(sumOfSD / list.size).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      (x._1, totalAvg, totalSD)
   })
   
   val x = totalMeanAndSD.toList
   x.toDF().repartition(1).write.mode(SaveMode.Overwrite).format("csv").save("/home/cloudera/workspace1/sparkproject/titanic_output")
   println("Task Completed!")
  }
}