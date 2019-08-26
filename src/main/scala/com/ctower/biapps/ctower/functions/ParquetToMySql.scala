package com.biapps.ctower.functions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode

object ParquetToMySql {
  val URL = "jdbc:mysql://172.18.114.95:3306/pawanProdDBTest";
  val userName = "pawan123";
  val password = "Pawan@123##";
  val driver = "com.mysql.jdbc.Driver"
  val tableName = "AWB_MST"

  def persistInMySql(dataframe: DataFrame): Unit = {
    try {
      val prop = new java.util.Properties()
      prop.put("user", userName)
      prop.put("password", password)
      prop.put("driver", driver)
      dataframe.write.mode(SaveMode.Append).jdbc(driver, tableName, prop)
      println("*** Record persisted ***")
    } catch {
      case e: Exception => e.printStackTrace
    }
  }
}