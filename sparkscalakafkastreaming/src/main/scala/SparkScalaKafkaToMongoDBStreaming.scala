import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.functions._

object SparkScalaKafkaToMongoDBStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .config("es.index.auto.create", "true")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/sparkscalacassandra.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/sparkscalacassandra.myCollection")
      .getOrCreate()
/*
  val streamedDataFrame= spark.readStream
     .format("kafka")
     .option("kafka.bootstrap.servers", "localhost:9092")
     .option("subscribe", "inputjsontokafkatopic")
     .load
     .selectExpr("CAST(value AS STRING)")*/
    import spark.implicits._ // << add this
    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "inputkafkatopictojson")
      .load()

    val employeeJsonDf = inputDf.selectExpr("CAST(value AS STRING)")
    val struct = new StructType()
      .add("empid", DataTypes.StringType)
      .add("empname", DataTypes.StringType)
      .add("empsal", DataTypes.StringType)

    val  employeeNestedDf = employeeJsonDf.select(from_json($"value", struct).as("employee"))
    val employeeFlattenedDf = employeeNestedDf.selectExpr("employee.empid", "employee.empname", "employee.empsal")

   /* val consoleOutput = inputDf.writeStream
      .outputMode("append")
      .format("console")
      .start()
    consoleOutput.awaitTermination()
*/

    employeeJsonDf.writeStream
      .outputMode("append")
      .format("com.mongodb.spark.sql")
      .option("checkpointLocation", "src/main/resources/sparkscalamongodb")
      .option("collection", "employee")
      .start().awaitTermination()


  }
}
