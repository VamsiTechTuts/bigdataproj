import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkScalaReadFromCassandra {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()

    val employeeDataFrame=spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "employee", "keyspace" -> "sparkcassandra")).load()
    employeeDataFrame.show(false)

  }
}
