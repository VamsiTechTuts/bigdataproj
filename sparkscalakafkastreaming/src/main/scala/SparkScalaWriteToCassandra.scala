import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkScalaWriteToCassandra {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()

    val empSchema=StructType(Seq(
      StructField("empid", StringType, nullable=true),
      StructField("empname", StringType, nullable=true),
      StructField("empsal", StringType, nullable=true)
    ))
    val employeeStructuredStream=spark.read.format("json")
      .schema(empSchema)
      .load("src/main/resources/inputjson/employee1.json")
    employeeStructuredStream.write.format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "employee", "keyspace" -> "sparkcassandra")).save()

  }
}
