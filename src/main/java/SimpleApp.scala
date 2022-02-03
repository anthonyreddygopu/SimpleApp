/* SimpleApp.scala
* spark-submit --class SimpleApp gopu-project-1.0.jar */
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object SimpleApp {
  def main(args: Array[String]): Unit = {
   val logFile = "C:/Users/Spark/README.md" // Should be some file on your system
   val spark = SparkSession
     .builder
     .appName("Simple Application")
     .config("spark.master", "local")
     .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    println("Anthony Reddy G-opu")
    spark.stop()
  }
}



