package com.qtech.bigdata.util

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat
import java.util.Date

import com.qtech.bigdata.comm.AppConstants._
import com.qtech.bigdata.comm.SendEMailWarning

object ConnectionImpala{
  /**
   * 获取 Impala 连接
   *
   * @param url
   * @return
   */
  def getConnection(url: String): Connection = {
    var conn: Connection = null
    try {
      println("------------before connect----------------------")
      Class.forName("com.cloudera.impala.jdbc41.Driver")
      conn = DriverManager.getConnection(url)
      println(s"------------get connect ${conn.isClosed}----------------------")

    } catch {
      case e: Exception => {
        println(e.getMessage)
        SendEMailWarning.sendMail(RECEIVE_EMAIL, s"${this.getClass.getName} Job failed", s"data-etl run Class  ${this.getClass.getName} getConnection(): \r\n ${e.getStackTrace} ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} \r\n  ${e.getMessage} \n")

      }

    }
    conn
  }


  /**
   * 关闭连接
   *
   * @param conn
   */
  def closeConn(conn: Connection): Unit = {
    try {
      if (conn != null) {
        conn.close()
      }
    } catch {
      case e: Exception => {
        println(e.getMessage)
        SendEMailWarning.sendMail(RECEIVE_EMAIL, s"${this.getClass.getName} Job failed", s"run Class ${this.getClass.getName} getConnection(): \r\n ${e.getStackTrace} ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())} \r\n  ${e.getMessage} \n")
      }
    }
  }
}
