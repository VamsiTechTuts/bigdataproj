import org.apache.spark.sql.SparkSession

object HelloWorld {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]")
     //cd .. .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()

  }
}
