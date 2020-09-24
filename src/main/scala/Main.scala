import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/hadoop")
    val spark = SparkSession.builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }
}
