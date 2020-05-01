import com.datastax.spark.connector.SomeColumns
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.datastax.spark.connector._
object SparkScalaJsonStrcutruedStreamingCassandra {
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

    /*employeeStructuredStream.writeStream
    .option("checkpointLocation", "src/main/resources/sparkscalacassandra")
      .format("org.apache.spark.sql.cassandra")\
    .option("keyspace", "analytics")\
    .option("table", "test")\
    .start()*/
   /* employeeStructuredStream.writeStream
      .option("checkpointLocation", "src/main/resources/sparkscalacassandra")
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "sparkscalacassandra")
      .option("table", "employee")
      .start()
      .awaitTermination()
  */
    //employeeStructuredStream.saveToCassandra("test", "words", SomeColumns("word", "count"))
    employeeStructuredStream.write.format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "employee", "keyspace" -> "sparkscalacassandra")).save()

  }
}
