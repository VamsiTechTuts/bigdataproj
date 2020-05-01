package com.companyname.postgresqltomysql

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkScalaReadFromPostgresqlWritetoMysql {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]")
      .getOrCreate()
    val employeeDataFrameFromJson=spark.read.format("json")
      .load("src/main/resources/inputjson/employee1.json")
      employeeDataFrameFromJson.show(false)
   /* employeeDataFrameFromJson.write
//        .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mysql")
      .option("dbtable", "employee")
      .option("user", "root")
      .option("password", "srinadh")
   */

    //create properties object
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "srinadh")

    //jdbc mysql url - destination database is named "data"
    val url = "jdbc:mysql://localhost:3306/mysql"

    //destination database table
    val table = "employee"

    //write data from spark dataframe to database
    employeeDataFrameFromJson.write.mode("append").jdbc(url, table, prop)

  }
}
