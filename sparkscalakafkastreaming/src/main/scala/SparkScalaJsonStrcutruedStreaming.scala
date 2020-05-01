import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkScalaJsonStrcutruedStreaming {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]").getOrCreate()

    val empSchema=StructType(Seq(
      StructField("empid", StringType, nullable=true),
      StructField("empname", StringType, nullable=true),
      StructField("empsal", StringType, nullable=true)
    ))

    //val employeeDataFrame=spark.read.format("json").load("src/main/resources/employee1.json")
    /*val employeeStructuredStream=spark.readStream.format("json")
      .schema(empSchema)
      .load("src/main/resources/inputjson")*/
    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
    val consoleOutput = inputDf.writeStream
      .outputMode("append")
      .format("console")
      .start()
    consoleOutput.awaitTermination()
   /* employeeStructuredStream.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("group.id","sparkscalajsonkafka")
      .option("topic", "test")
      .option("checkpointLocation", "src/main/resources/checkpointlocation")
      .start()
      .awaitTermination()
 */ }
}
