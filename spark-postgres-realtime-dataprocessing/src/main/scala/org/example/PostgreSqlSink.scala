package org.example

import java.sql._
import org.json4s._
import org.json4s.native.JsonMethods._


case class Employee(empid: String, empname: String, empsal: String)

class PostgreSqlSink(url: String, user: String, pwd: String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
  val driver = "org.postgresql.Driver"
  var connection: java.sql.Connection = _
  var statement: java.sql.PreparedStatement = _
  val v_sql = "insert INTO employee(empid, empname, empsal) values(?,?,?)"
  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = java.sql.DriverManager.getConnection(url, user, pwd)
    connection.setAutoCommit(false)
    statement = connection.prepareStatement(v_sql)
    true
  }
  def process(value: org.apache.spark.sql.Row): Unit = {
    implicit val formats = DefaultFormats
    val employee=parse(value.get(0).toString).extract[Employee]



    print(value)
    statement.setString(1,employee.empid)
    statement.setString(2,employee.empname)
    statement.setString(3,employee.empsal)
    statement.executeUpdate()
   // val jsonMap = parse(jsonString).values.asInstanceOf[Map[String, Any]]

    // ignoring value(0) as this is address
   /* statement.setString(1, value(1).toString)
    statement.setString(2, value(2).toString)
    statement.setString(3, value(3).toString)
    statement.executeUpdate()*/
  }
  def close(errorOrNull: Throwable): Unit = {
    connection.commit()
    connection.close
  }
}