package com.biapps.ctower.functions

import java.security.PrivilegedExceptionAction
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}


object PLCleanUp {

  val conf = new SparkConf().setAppName("PLDataCleanUp").setMaster("yarn-cluster")
  conf.set("spark.dynamicAllocation.enabled", "false")
  conf.set("spark.driver.memory", "40g")
  conf.set("spark.executor.memory", "16g")
  conf.set("spark.executor.cores", "4")
  conf.set("spark.executor.instances", "30")
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

  def main(args: Array[String]): Unit = {
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
    val HbaseSchema = StructType(
      List(
        StructField("PK", StringType),
        StructField("CT_COL", StringType)))

    val loginUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM", "hbase.keytab")
    println("Login user created :>> " + loginUser)
    loginUser.doAs(new PrivilegedExceptionAction[Unit] {
      override def run: Unit = {
        //creating a configuration object

        val sdf = new SimpleDateFormat(dateFmt)
        val config = HBaseConfiguration.create()
        config.set("hbase.zookeeper.quorum", "172.18.114.95")
        config.set("hbase.zookeeper.property.clientPort", "2181")
        config.set("hbase.regionserver.compaction.enabled","false")

        println(">>> config >>> " + config)
        val PL_tableName_AWBNO = "CTOWER.CT_PL_CAWBNO"

        config.set(TableInputFormat.INPUT_TABLE,PL_tableName_AWBNO)
        import sqlContext.implicits._
        val closure_codes = "'123','016','026','132','074','000','135','141','021','025','099','105','070','090','098','188','178','284'"


        val hBasePL = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        val df_hbasepl = hBasePL.map(x => {
          (
            Bytes.toString(x._2.getRow),
            Bytes.toString(x._2.getValue(Bytes.toBytes("PDJ"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("ABM"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("SLT"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("DLS"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("DTS"), Bytes.toBytes("CT_COL")))
          )
        }).toDF().withColumnRenamed("_1", "PK").withColumnRenamed("_2", "PDJ").withColumnRenamed("_3", "ABM").withColumnRenamed("_4", "SLT").withColumnRenamed("_5", "DLS").withColumnRenamed("_6", "DTS")

        df_hbasepl.registerTempTable("plhbase")
        var dls_DF1:DataFrame = null;
        var delivery_date2:DataFrame = null;

        dls_DF1 = sqlContext.sql(s"""Select PK, NVL(CASE WHEN (ABM = "NA!@#" or ABM is null) AND (SLT = "NA!@#" or SLT is null) AND (PDJ = "NA!@#" or PDJ IS NULL) THEN 'I' ELSE CASE WHEN (ABM = "NA!@#" or ABM is null) AND (SLT = "NA!@#" or SLT is null) AND (PDJ != "NA!@#" or PDJ is NOT NULL) THEN CASE WHEN (instr(PDJ,'S015') <= 0 AND instr(PDJ,'S018') <= 0 ) THEN 'I' ELSE CASE WHEN (instr(PDJ,'S015') > 0 OR instr(PDJ,'S018') > 0 ) AND (DATEDIFF(CURRENT_DATE(), CAST(substr(PDJ, instr(PDJ, '~S015~') - 19, 16) AS  DATE)) > 45 OR DATEDIFF(CURRENT_DATE(), CAST(substr(PDJ, instr(PDJ, '~S018~') - 19, 16) AS DATE)) > 45) THEN 'I' ELSE 'UD' END END ELSE
CASE WHEN (ABM != "NA!@#" or ABM is NOT NULL OR SLT != "NA!@#" or SLT is NOT NULL) AND (PDJ = "NA!@#" or PDJ IS NULL) THEN 'NA!@#' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T123~') > 0 THEN substr(PDJ,instr(PDJ, '~T123~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T016~') > 0 THEN substr(PDJ,instr(PDJ, '~T016~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T026~') > 0 THEN substr(PDJ,instr(PDJ, '~T026~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T132~') > 0 THEN substr(PDJ,instr(PDJ, '~T132~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T074~') > 0 THEN substr(PDJ,instr(PDJ, '~T074~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T000~') > 0 THEN substr(PDJ,instr(PDJ, '~T000~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T135~') > 0 THEN substr(PDJ,instr(PDJ, '~T135~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T141~') > 0 THEN substr(PDJ,instr(PDJ, '~T141~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T021~') > 0 THEN substr(PDJ,instr(PDJ, '~T021~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T025~') > 0 THEN substr(PDJ,instr(PDJ, '~T025~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T099~') > 0 THEN substr(PDJ,instr(PDJ, '~T099~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T105~') > 0 THEN substr(PDJ,instr(PDJ, '~T105~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T070~') > 0 THEN substr(PDJ,instr(PDJ, '~T070~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T090~') > 0 THEN substr(PDJ,instr(PDJ, '~T090~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T098~') > 0 THEN substr(PDJ,instr(PDJ, '~T098~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T188~') > 0 THEN substr(PDJ,instr(PDJ, '~T188~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T178~') > 0 THEN substr(PDJ,instr(PDJ, '~T178~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T284~') > 0 THEN substr(PDJ,instr(PDJ, '~T284~')+1,4) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T123~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T016~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T026~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T132~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T074~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T000~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T135~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T141~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T021~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T025~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T099~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T105~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T070~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T090~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T098~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T188~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T178~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T284~') <=0 AND (DLS IS NULL OR SUBSTR(DLS,1,3) NOT IN ($closure_codes) OR DLS = 'I' OR DLS = 'NA!@#') THEN 'UD' ELSE
DLS END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END END ,'NA!@#') as CT_COL from plhbase""")

        delivery_date2 = sqlContext.sql(s"""Select PK, NVL(CASE WHEN (ABM = "NA!@#" or ABM is null) AND (SLT = "NA!@#" or SLT is null) AND (PDJ = "NA!@#" or PDJ IS NULL) THEN from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') ELSE
CASE WHEN (ABM = "NA!@#" or ABM is null) AND (SLT = "NA!@#" or SLT is null) AND (PDJ != "NA!@#" or PDJ is NOT NULL) THEN
CASE WHEN (instr(PDJ,'S015') <= 0 AND instr(PDJ,'S018') <=0 ) THEN from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') ELSE
CASE WHEN (instr(PDJ,'S015') > 0 OR instr(PDJ,'S018') > 0 ) AND (DATEDIFF(CURRENT_DATE(), CAST(substr(PDJ, instr(PDJ, '~S015~') - 19, 10) AS  DATE)) > 45 OR DATEDIFF(CURRENT_DATE(), CAST(substr(PDJ, instr(PDJ, '~S018~') - 19, 10) AS DATE)) > 45) THEN from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm') ELSE
CASE WHEN locate("|",reverse(PDJ)) >0 THEN substr(PDJ,length(PDJ) - locate("|",reverse(PDJ))+2,16) ELSE  substr(PDJ,0,16) END END END ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T123~') > 0 THEN substr(PDJ,instr(PDJ, '~T123~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T016~') > 0 THEN substr(PDJ,instr(PDJ, '~T016~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T026~') > 0 THEN substr(PDJ,instr(PDJ, '~T026~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T132~') > 0 THEN substr(PDJ,instr(PDJ, '~T132~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T074~') > 0 THEN substr(PDJ,instr(PDJ, '~T074~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T000~') > 0 THEN substr(PDJ,instr(PDJ, '~T000~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T135~') > 0 THEN substr(PDJ,instr(PDJ, '~T135~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T141~') > 0 THEN substr(PDJ,instr(PDJ, '~T141~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T021~') > 0 THEN substr(PDJ,instr(PDJ, '~T021~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T025~') > 0 THEN substr(PDJ,instr(PDJ, '~T025~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T099~') > 0 THEN substr(PDJ,instr(PDJ, '~T099~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T105~') > 0 THEN substr(PDJ,instr(PDJ, '~T105~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T070~') > 0 THEN substr(PDJ,instr(PDJ, '~T070~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T090~') > 0 THEN substr(PDJ,instr(PDJ, '~T090~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T098~') > 0 THEN substr(PDJ,instr(PDJ, '~T098~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T188~') > 0 THEN substr(PDJ,instr(PDJ, '~T188~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T178~') > 0 THEN substr(PDJ,instr(PDJ, '~T178~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND instr(PDJ, '~T284~') > 0 THEN substr(PDJ,instr(PDJ, '~T284~')-19,16) ELSE
CASE WHEN ((ABM != "NA!@#" or ABM is NOT NULL) OR (SLT != "NA!@#" or SLT is NOT NULL)) AND (PDJ != "NA!@#" or PDJ is NOT NULL) THEN CASE WHEN locate("|",reverse(PDJ)) >0 THEN substr(PDJ,length(PDJ) - locate("|",reverse(PDJ))+2,16) ELSE substr(PDJ,0,16) END  ELSE
DTS END END END END END END END END END END END END END END END END END END END END END ,'NA!@#') as CT_COL from plhbase""")

        if(dls_DF1== null){
          println("dls_DF1 is null, creating empty dataframe")
          dls_DF1 = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }
        if(delivery_date2== null){
          println("delivery_date2 is null, creating empty dataframe")
          delivery_date2 = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }

        val pl_map_dlv = Map("DLS" -> dls_DF1,"DTS" -> delivery_date2)

        //val PL_DLS_TEST = "CTDEV.TEST_DLS_DTS"

        println("PL Table dump start::::: " + sdf.format(new Date()))

        pl_map_dlv.foreach { a: (String, DataFrame) =>
          val key = a._1
          val value = a._2

          println(s"key: $key ")

          def PL_catalog = s"""{
                              |"table":{"namespace":"default", "name":"${PL_tableName_AWBNO}"},
                              |"rowkey":"PK",
                              |"columns":{
                              |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                              |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                              |}
                              |}""".stripMargin

          println("catalog created :::: " + PL_catalog)

          println(sdf.format(new Date()))

          value.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

          println("catalog " + key + " written to hbase table:::: " + sdf.format(new Date()))

        }

        /*val PL_tableName_AWBNO = "CTOWER.CT_PL_CAWBNO"
        config.set(TableInputFormat.INPUT_TABLE, PL_tableName_AWBNO)

        val hBaseRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        import sqlContext.implicits._

        val df_hbasetab = hBaseRDD.map(x => {
          (
            Bytes.toString(x._2.getRow),
            Bytes.toString(x._2.getValue(Bytes.toBytes("PDJ"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("DLS"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("ABM"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("SLT"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("IBS"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("OBS"), Bytes.toBytes("CT_COL")))
          )
        }).toDF().withColumnRenamed("_1", "PK").withColumnRenamed("_2", "PDJ").withColumnRenamed("_3", "DLS").withColumnRenamed("_4", "ABM").withColumnRenamed("_5", "SLT").withColumnRenamed("_6", "IBS").withColumnRenamed("_7", "OBS")


        df_hbasetab.registerTempTable("hbasetab")

        val IncrLayer_DF1 = sqlContext.sql("""SELECT PK , PDJ AS pod_journey FROM hbasetab where DLS = 'UD'""")
        IncrLayer_DF1.registerTempTable("hbaseUD")


        val df1 = sqlContext.sql("""select PK ,pod_journey,case when locate('T123',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T123',pod_journey),4) ELSE
        case when locate('T016',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T016',pod_journey),4) ELSE
        case when locate('T026',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T026',pod_journey),4) ELSE
        case when locate('T132',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T132',pod_journey),4) ELSE
        case when locate('T074',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T074',pod_journey),4) ELSE
        case when locate('T000',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T000',pod_journey),4) ELSE
        case when locate('T135',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T135',pod_journey),4) ELSE
        case when locate('T141',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T141',pod_journey),4) ELSE
        case when locate('T021',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T021',pod_journey),4) ELSE
        case when locate('T025',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T025',pod_journey),4) ELSE
        case when locate('T099',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T099',pod_journey),4) ELSE
        case when locate('T105',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T105',pod_journey),4) ELSE
        case when locate('T070',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T070',pod_journey),4) ELSE
        case when locate('T090',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T090',pod_journey),4) ELSE
        case when locate('T098',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T098',pod_journey),4) ELSE
        case when locate('T188',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T188',pod_journey),4) ELSE
        case when locate('T178',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T178',pod_journey),4) ELSE
        case when locate('T284',pod_journey)>0 THEN SUBSTR(pod_journey,locate('T284',pod_journey),4) ELSE 'UD'
        END END END END END END END END END END END END END END END END END END AS delstat from hbaseUD
        """)

        //println("UD records with closure codes:"+df1.filter('delstat!=="UD").count)
        //df1.filter('delstat!=="UD").show(50,false)
        df1.registerTempTable("closurecode")

        val closure_df = sqlContext.sql("""SELECT PK,delstat as CT_COL from closurecode""")

        val pl_map2 = Map("DLS" -> closure_df)

        pl_map2.foreach { a: (String, DataFrame) =>
          val key = a._1
          val value = a._2

          println(s"key: $key ")

          def PL_catalog2 = s"""{
                               |"table":{"namespace":"default", "name":"${PL_tableName_AWBNO}"},
                               |"rowkey":"PK",
                               |"columns":{
                               |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                               |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                               |}
                               |}""".stripMargin

          println("catalog created :::: " + PL_catalog2)

          println(sdf.format(new Date()))

          value.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog2, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

          println("catalog " + key + " written to hbase table:::: " + sdf.format(new Date()))

        }

        val IncrLayer_DF = sqlContext.sql("""SELECT PK , PDJ AS pod_journey FROM hbasetab""")

        IncrLayer_DF.registerTempTable("after_read")

        val df2 = sqlContext.sql("""select PK,case when instr(pod_journey, '~T123~') > 0 then substr(pod_journey,instr(pod_journey, '~T123~')-19,16) when instr(pod_journey, '~T016~') > 0 then substr(pod_journey,instr(pod_journey, '~T016~')-19,16) when instr(pod_journey, '~T026~') > 0 then substr(pod_journey,instr(pod_journey, '~T026~')-19,16) when instr(pod_journey, '~T132~') > 0 then substr(pod_journey,instr(pod_journey, '~T132~')-19,16) when instr(pod_journey, '~T074~') > 0 then substr(pod_journey,instr(pod_journey, '~T074~')-19,16) when instr(pod_journey, '~T000~') > 0 then substr(pod_journey,instr(pod_journey, '~T000~')-19,16) when instr(pod_journey, '~T135~') > 0 then substr(pod_journey,instr(pod_journey, '~T135~')-19,16) when instr(pod_journey, '~T141~') > 0 then substr(pod_journey,instr(pod_journey, '~T141~')-19,16) when instr(pod_journey, '~T021~') > 0 then substr(pod_journey,instr(pod_journey, '~T021~')-19,16) when instr(pod_journey, '~T025~') > 0 then substr(pod_journey,instr(pod_journey, '~T025~')-19,16) when instr(pod_journey, '~T099~') > 0 then substr(pod_journey,instr(pod_journey, '~T099~')-19,16) when instr(pod_journey, '~T105~') > 0 then substr(pod_journey,instr(pod_journey, '~T105~')-19,16) when instr(pod_journey, '~T070~') > 0 then substr(pod_journey,instr(pod_journey, '~T070~')-19,16) when instr(pod_journey, '~T090~') > 0 then substr(pod_journey,instr(pod_journey, '~T090~')-19,16) when instr(pod_journey, '~T098~') > 0 then substr(pod_journey,instr(pod_journey, '~T098~')-19,16) when instr(pod_journey, '~T188~') > 0 then substr(pod_journey,instr(pod_journey, '~T188~')-19,16) when instr(pod_journey, '~T178~') > 0 then substr(pod_journey,instr(pod_journey, '~T178~')-19,16) when instr(pod_journey, '~T284~') > 0 then substr(pod_journey,instr(pod_journey, '~T284~')-19,16) end AS dlvstatdate, pod_journey from  after_read""")
        df2.registerTempTable("dtsupdate")

        val closure_dts = sqlContext.sql("""SELECT PK,dlvstatdate as CT_COL from dtsupdate where dlvstatdate is not null""")
        val pl_map3 = Map("DTS" -> closure_dts)

        pl_map3.foreach { a: (String, DataFrame) =>
          val key = a._1
          val value = a._2

          println(s"key: $key ")

          def PL_catalog2 = s"""{
                               |"table":{"namespace":"default", "name":"${PL_tableName_AWBNO}"},
                               |"rowkey":"PK",
                               |"columns":{
                               |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                               |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                               |}
                               |}""".stripMargin

          println("catalog created :::: " + PL_catalog2)

          println(sdf.format(new Date()))

          value.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog2, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

          println("catalog " + key + " written to hbase table:::: " + sdf.format(new Date()))

        }



        val ignoreDF = sqlContext.sql("""select PK,ABM,SLT,PDJ,IBS,OBS from hbasetab where (ABM = "NA!@#" or ABM is null) AND (SLT = "NA!@#" or SLT is null) AND (PDJ != "NA!@#" or PDJ is not null) and DLS = 'UD'""")
        ignoreDF.registerTempTable("oldrecords")
        val oldrecords_df = sqlContext.sql("""select PK,SPLIT(OBS,'~')[9] AS OBS_DSTATDATE,SPLIT(IBS,'~')[6] AS IBS_DSTATDATE,ABM,SLT,PDJ from oldrecords where (instr(PDJ,'S015') <= 0 AND instr(PDJ,'S018') <=0 )""")
        //oldrecords_df.show(30,false)
        oldrecords_df.registerTempTable("oldrec")
        val ignoreHbaseDF = sqlContext.sql("""SELECT PK,'I' as CT_COL from oldrec""")

        val pl_map1 = Map("DLS" -> ignoreHbaseDF)

        pl_map1.foreach { a: (String, DataFrame) =>
          val key = a._1
          val value = a._2

          println(s"key: $key ")

          def PL_catalog2 = s"""{
                               |"table":{"namespace":"default", "name":"${PL_tableName_AWBNO}"},
                               |"rowkey":"PK",
                               |"columns":{
                               |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                               |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                               |}
                               |}""".stripMargin

          println("catalog created :::: " + PL_catalog2)

          println(sdf.format(new Date()))

          value.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog2, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

          println("catalog " + key + " written to hbase table:::: " + sdf.format(new Date()))

        }
        val UD_IRecords = sqlContext.sql("""select PK,PDJ from hbasetab where DLS in ('I','UD')""")
        UD_IRecords.registerTempTable("UDIrecords")

        val dlsupdate_DF = sqlContext.sql("""select PK,case when locate("|",reverse(PDJ)) >0 then substr(PDJ,length(PDJ) - locate("|",reverse(PDJ))+2,16) else  substr(PDJ,0,16) END as CT_COL from UDIrecords where PDJ is not null or PDJ!='NA!@#'""")

        dlsupdate_DF.show(50,false)

        val pl_map4 = Map("DTS" -> dlsupdate_DF)

        pl_map4.foreach { a: (String, DataFrame) =>
          val key = a._1
          val value = a._2

          println(s"key: $key ")

          def PL_catalog2 = s"""{
                               |"table":{"namespace":"default", "name":"${PL_tableName_AWBNO}"},
                               |"rowkey":"PK",
                               |"columns":{
                               |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                               |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                               |}
                               |}""".stripMargin

          println("catalog created :::: " + PL_catalog2)

          println(sdf.format(new Date()))

          value.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog2, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

          println("catalog " + key + " written to hbase table:::: " + sdf.format(new Date()))

        }*/


      }

})
  }
}
