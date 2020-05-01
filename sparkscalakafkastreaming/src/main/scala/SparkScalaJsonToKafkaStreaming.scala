import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkScalaJsonToKafkaStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()
    val mySchema = StructType(Array(
      StructField("empid", StringType),
      StructField("empname", StringType),
      StructField("empsal", StringType)
    ))
    val streamingDataFrame = spark.readStream.schema(mySchema).json("src/main/resources/inputjson")

    streamingDataFrame.selectExpr("CAST(empid AS STRING) AS key", "to_json(struct(*)) AS value").
      writeStream
      .format("kafka")
      .option("topic", "inputjsontokafkatopic")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "src/main/resources/inputjsonkafkacheckpont")
      .start()
      .awaitTermination()
  }
}
