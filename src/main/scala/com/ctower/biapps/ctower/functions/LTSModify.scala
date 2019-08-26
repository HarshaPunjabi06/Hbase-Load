package com.biapps.ctower.functions

import org.apache.spark.sql.DataFrame
import java.security.PrivilegedExceptionAction
import java.text.SimpleDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog

  object LTSModify {

  val conf = new SparkConf().setAppName("LTS_FormatChange_PL").setMaster("yarn-cluster")
  conf.set("spark.dynamicAllocation.enabled", "false")
  conf.set("spark.driver.memory", "20g")
  conf.set("spark.executor.memory", "8g")
  conf.set("spark.executor.cores", "2")
  conf.set("spark.executor.instances", "10")
  conf.set("spark.speculation", "true")
  conf.set("spark.yarn.maxAppAttempts", "4")
  conf.set("spark.yarn.am.attemptFailuresValidityInterval", "1h")
  conf.set("spark.task.maxFailures", "8")
  conf.set("principal", "hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM")
  conf.set("keytab", "hbase.keytab")
  conf.set("hbase.security.authentication", "kerberos")

  val sc = new SparkContext(conf)
  val groupId = "console-consumer-10007"
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  def main(args: Array[String]) {

    System.setProperty("java.security.krb5.conf", "krb5.conf");
    System.setProperty("java.security.krb5.realm", "BLUEDART.COM");
    System.setProperty("java.security.krb5.kdc", "SINHQCDC11.bluedart.com");

    val configuration = new Configuration()
    configuration.set("hadoop.security.authentication", "kerberos");
    configuration.set("hbase.rest.authentication.kerberos.keytab", "/home/datalake/ControlTowerProd/CommonFiles/keytab/hbase.keytab");
    configuration.set("hbase.master.keytab.file", "/home/datalake/ControlTowerProd/CommonFiles/keytab/hbase.keytab");
    configuration.set("hbase.regionserver.keytab.file", "/home/datalake/ControlTowerProd/CommonFiles/keytab/hbase.keytab");
    configuration.set("hbase.cluster.distributed", "true");
    configuration.set("hbase.rpc.protection", "authentication");
    configuration.set("hbase.client.retries.number", "5");
    configuration.set("hbase.regionserver.kerberos.principal", "hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM");
    configuration.set("hbase.master.kerberos.principal", "hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM");
    UserGroupInformation.setConfiguration(configuration);
    val dateFmt = "dd-MMM-YYYY HH:mm:ss"

    val loginUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM", "hbase.keytab")
    println("Login user created :>> " + loginUser)
    loginUser.doAs(new PrivilegedExceptionAction[Unit] {
      override def run: Unit = {
        //creating a configuration object

        val sdf = new SimpleDateFormat(dateFmt)
        val config = HBaseConfiguration.create()
        //      config.clear()
        config.set("hbase.zookeeper.quorum", "172.18.114.95")
        config.set("hbase.zookeeper.property.clientPort", "2181")
        config.set("hbase.regionserver.compaction.enabled", "false")
        println(">>> config >>> " + config)


        val PL_tableName_AWBNO = "CTOWER.CT_PL_CAWBNO"
        config.set(TableInputFormat.INPUT_TABLE, PL_tableName_AWBNO)

        val hBaseRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        import sqlContext.implicits._

        val df_hbasetab = hBaseRDD.map(x => {
          (
            Bytes.toString(x._2.getRow),
            Bytes.toString(x._2.getValue(Bytes.toBytes("LTS"), Bytes.toBytes("CT_COL")))

          )
        }).toDF().withColumnRenamed("_1", "PK").withColumnRenamed("_2", "LTS")

        val df_filter = df_hbasetab.filter(expr("LTS like '20190%'"))

        val new_df = df_filter.withColumn("CT_COL", from_unixtime(unix_timestamp(col("LTS"), "yyyyMMddHHmm"), "yyyy-MM-dd HH:mm:ss"))

        val lts_hbase = new_df.select(col("PK"),col("CT_COL"))
        lts_hbase.show(20, false)

        def lts_catalog =
          s"""{
             |"table":{"namespace":"default", "name":"${PL_tableName_AWBNO}"},
             |"rowkey":"PK",
             |"columns":{
             |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
             |"CT_COL":{"cf":"LTS", "col":"CT_COL", "type":"string"}
             |}
             |}""".stripMargin


        println("catalog created :::: " + lts_catalog)

        lts_hbase.write.options(Map(HBaseTableCatalog.tableCatalog -> lts_catalog, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()
      }
    })
  }
}