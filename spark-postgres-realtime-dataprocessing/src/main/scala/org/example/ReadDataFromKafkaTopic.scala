package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
object ReadDataFromKafkaTopic {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("kafka-tutorials")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    val employeeJsonDf = inputDf.selectExpr("CAST(value AS STRING)")
    val struct = new StructType()
      .add("empid", DataTypes.StringType)
      .add("empname", DataTypes.StringType)
      .add("empsal", DataTypes.StringType)

    val  employeeNestedDf = employeeJsonDf.select(from_json($"value", struct).as("employee"))
    val employeeFlattenedDf = employeeNestedDf.selectExpr("employee.empid", "employee.empname", "employee.empsal")


    /* val payloadDf = inputDf.selectExpr("CAST(value AS STRING)").as[String]

     // split string into array of strings for each future column
     val split_col = payloadDf.withColumn("value", split(col("value"), ";"))
     // user regular expressions to extract the column data
     val empid = regexp_replace(split_col.col("value").getItem(0), "\\[\\w+\\]:", "")
     val empname = regexp_replace(split_col.col("value").getItem(1), "\\[\\w+\\]:", "")
     val empsal = regexp_replace(split_col.col("value").getItem(2), "\\[\\w+\\]:", "")

     print(empid)
     print(empname)
     print(empsal)*/

    val url = "jdbc:postgresql://localhost:5432/postgres"
    val user = "postgres"
    val pw = "postgres"
    val jdbcWriter = new PostgreSqlSink(url,user,pw)
   /* val res = split_col
      .withColumn("empid", empid)
      .withColumn("empname", empname)
      .withColumn("empsal", empsal)*/
    val writeData = employeeJsonDf.writeStream
      .foreach(jdbcWriter)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("append")
      .start()
    print("Starting...")
    writeData.awaitTermination()

    /*val consoleOutput = inputDf.writeStream
      .outputMode("append")
      .format("console")
      .start()
    consoleOutput.awaitTermination()*/
  }
}
