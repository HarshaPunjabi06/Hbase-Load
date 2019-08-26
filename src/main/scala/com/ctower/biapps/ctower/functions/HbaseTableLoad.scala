package com.biapps.ctower.functions

import java.security.PrivilegedExceptionAction
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.hive.HiveContext

class HbaseTableLoad {

  val dateFmt = "HH:mm:ss"

  def hbaseWrite(sc: SparkContext,df_map: Map[String, DataFrame],sqlContext: HiveContext): Unit = {

    println("*** InsideHbaseConnection *** ");
    System.setProperty("java.security.krb5.conf", "krb5.conf");
    System.setProperty("java.security.krb5.realm", "BLUEDART.COM");
    System.setProperty("java.security.krb5.kdc", "SINHQCDC11.bluedart.com");

    val configuration = new Configuration()
    configuration.set("hadoop.security.authentication", "kerberos");
    configuration.set("hbase.rest.authentication.kerberos.keytab", "//home/datalake/ControlTowerProd/CommonFiles/keytab/hbase.keytab");
    configuration.set("hbase.master.keytab.file", "/home/datalake/ControlTowerProd/CommonFiles/keytab/hbase.keytab");
    configuration.set("hbase.regionserver.keytab.file", "/home/datalake/ControlTowerProd/CommonFiles/keytab/hbase.keytab");
    configuration.set("hbase.cluster.distributed", "true");
    configuration.set("hbase.rpc.protection", "authentication");
    configuration.set("hbase.client.retries.number", "5");
    configuration.set("hbase.regionserver.kerberos.principal", "hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM");
    configuration.set("hbase.master.kerberos.principal", "hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM");
    UserGroupInformation.setConfiguration(configuration);
    val loginUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM", "hbase.keytab")
    println("Login user created :>> " + loginUser)

    loginUser.doAs(new PrivilegedExceptionAction[Unit] {
      override def run: Unit = {val config = HBaseConfiguration.create()
        //      config.clear()
        config.set("hbase.zookeeper.quorum", "172.18.114.95")
        config.set("hbase.zookeeper.property.clientPort", "2181")
        config.set("hbase.regionserver.compaction.enabled","false")
        //config.set(TableInputFormat.INPUT_TABLE, PL_tableName)

        println(">>> config >>> " + config)

        //Creating HBaseAdmin object
        val admin = new HBaseAdmin(config)

        val sdf = new SimpleDateFormat(dateFmt)
        println("Before writing to HBASE:: >>" + sdf.format(new Date()))

        val tableName_DEL = "CT_DEL_AWBNO"

        df_map.foreach { a: (String, DataFrame) =>
          val key = a._1
          val value = a._2
          println(s"key: $key ")

          if (key == "DEL") {

            def PL_catalog_DEL = s"""{
                                    |"table":{"namespace":"default", "name":"${tableName_DEL}"},
                                    |"rowkey":"PK",
                                    |"columns":{
                                    |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                                    |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                                    |}
                                    |}""".stripMargin

            println("catalog created :::: " + PL_catalog_DEL)

            println("Before writing to " + key + " PL_catalog_DEL:: >> " + sdf.format(new Date()))

            value.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog_DEL, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

            println("After writing to  " + key + " PL_catalog_DEL:: >> " + sdf.format(new Date()))

          }}
      }})

  }

}
