package org.example

import java.sql._
import org.json4s._
import org.json4s.native.JsonMethods._


/*case class Employee(empid: String, empname: String, empsal: String)*/
case class Order(orderDateTime: String, orderId: String, orderStatus: String)

class PostgreSqlSinkForOrder(url: String, user: String, pwd: String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
  val driver = "org.postgresql.Driver"
  var connection: java.sql.Connection = _
  var statement: java.sql.PreparedStatement = _
  /*private String orderDateTime;
  private String orderId;
  private String orderStatus;*/
  val v_sql = "insert INTO order_status(orderDateTime, orderId, orderStatus) values(?,?,?)"
  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = java.sql.DriverManager.getConnection(url, user, pwd)
    connection.setAutoCommit(false)
    statement = connection.prepareStatement(v_sql)
    true
  }
  def process(value: org.apache.spark.sql.Row): Unit = {
    implicit val formats = DefaultFormats
    //val employee=parse(value.get(0).toString).extract[Employee]
    val order=parse(value.get(0).toString).extract[Order]



    print(value)
    statement.setString(1,order.orderDateTime)
    statement.setString(2,order.orderId)
    statement.setString(3,order.orderStatus)
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