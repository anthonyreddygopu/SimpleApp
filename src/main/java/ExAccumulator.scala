// https://www.edureka.co/blog/spark-accumulators-explained

import org.apache.spark.sql.SparkSession

object ExAccumulator  {
  def main(args: Array[String]): Unit = {
    val logFile = "D:/Projects/SimpleApp/Data/kv.txt" // Should be some file on your system
    val spark = SparkSession
      .builder
      .appName("Example on Accumulators")
      .config("spark.master", "local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")


    // Example without Accumulator  --> Output : Blank Lines = 0
    var blankLines1 : Int = 0
    spark.sparkContext.textFile(logFile).foreach{
      line => if (line.length() == 0) blankLines1 += 1
    }
    println(s"\tBlank Lines=$blankLines1")


    // Example with Accumulator  --> Output : Blank Lines = 2
    val blankLines  = spark.sparkContext.accumulator(0,"Blank Lines")
    spark.sparkContext.textFile(logFile,4).foreach{
      line => if (line.length() == 0) blankLines += 1
    }
    println(s"\tBlank Lines=${blankLines.value}")
    spark.stop()

  }
}



