import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  System.setProperty("hadoop.home.dir", "C:/hadoop")
  val spark: SparkSession = SparkSession.builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  implicit class implicitClass(df: DataFrame) {

    def toJsonArray(length: Int, cols: String*): DataFrame = df
      .withColumn("new_index", monotonically_increasing_id divide length cast "int")
      .groupBy("new_index")
      .agg(to_json(collect_list(struct(cols map col: _*))) as "json")
      .drop("new_index")
  }

  def main(args: Array[String]): Unit = {
    spark.read.format("csv")
      .option("header", "true")
      .load("src/main/resources/data.csv")
      .toJsonArray(2, "col1", "col2", "col3")
      .show()
  }
}
