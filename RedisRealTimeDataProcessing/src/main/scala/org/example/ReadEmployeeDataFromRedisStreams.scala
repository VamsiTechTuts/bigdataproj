package org.example

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.io.Source

object ReadEmployeeDataFromRedisStreams {
  def main(args: Array[String]): Unit = {
    var properties : Properties = null
    val url = getClass.getResource("/application-dev.properties")
    if (url != null) {
      val source = Source.fromURL(url)

      properties = new Properties()
      properties.load(source.bufferedReader())
    }
    //val redishost=properties.get("spark.redis.host")
    val spark = SparkSession
      .builder()
      .appName("redis-example")
      .master("local[*]")
      .config("spark.redis.host", "54.167.203.91")
      .config("spark.redis.port", "6379")
      .getOrCreate()
    val employee = spark
      .readStream
      .format("redis")
      .option("stream.keys", "employee")
      .schema(StructType(Array(
        StructField("empid", StringType),
        StructField("empname", StringType),
        StructField("empsal", LongType)
      )))
      .load()

    val clickWriter: ClickForeachWriter =
      new ClickForeachWriter("54.167.203.91", "6379")

    val query = employee
      .writeStream
      .outputMode("update")
      .foreach(clickWriter)
      .start()

    query.awaitTermination()
  }
}
