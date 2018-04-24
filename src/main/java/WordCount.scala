import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SQLContext}

object WordCount extends App{ override def  main(args: Array[String]){
    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
    val sc = new SparkContext(conf)
  
    val textFile = sc.textFile("/home/cloudera/workspace1/sparkproject/input/text.txt")
    val counts = textFile.flatMap(line => line.split(" "))
                 		.map(word => (word, 1))
			.reduceByKey(_ + _)
    counts.saveAsTextFile("/home/cloudera/workspace1/sparkproject/output")
  }
}