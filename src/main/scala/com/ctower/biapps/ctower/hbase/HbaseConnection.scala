package com.biapps.ctower.hbase

import java.security.PrivilegedExceptionAction
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import java.util.Properties
import org.apache.hadoop.fs.{FSDataInputStream,FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import com.ctower.util.ConstantHbase._

class HbaseConnection {

  val dateFmt = "HH:mm:ss"
  val dateFmt1 = "yyyy-MM-dd HH:mm"

  //def persistInHbase(sc: SparkContext, df: DataFrame, colf_name : String, sqlContext: HiveContext): Unit = {
/*
  'HbaseDelete' function is used to delete data from Hbase by passing a list od awbno's(PK)
  This is not used currently in IL/PL load functionality
*/
  /*def HbaseDelete(tableName: String, deleteRow: List[String]): Unit = {
    val conf = HBaseConfiguration.create();
    val table = new HTable(conf, tableName);
    for (i <- deleteRow) {
      var delete_record = new Delete(Bytes.toBytes(i));
      table.delete(delete_record)
    }
  }*/

  def truncateHbaseTable(tabname: String,admin: HBaseAdmin) :Unit ={

    if(admin.isTableEnabled(TableName.valueOf(tabname)) == true){
      println("Table enabled..Disabling it")
      admin.disableTable(TableName.valueOf(tabname))
    }
    else{
      println("Table disabled..First Enable then Disable it")
      admin.enableTable(TableName.valueOf(tabname))
      admin.disableTable(TableName.valueOf(tabname))
    }
    println("Truncating table:"+tabname)
    admin.truncateTable(TableName.valueOf(tabname), true)

  }

  def persistInHbase(sc: SparkContext, df_map: Map[String, DataFrame], sqlContext: HiveContext): Unit = {
    println("*** InsideHbaseConnection *** ")
    val prop = new Properties()
    val hdfsConfig = new Configuration()
    val propFilepath = new Path(PROPFILE)
    val hdfsfileSystem = FileSystem.get(hdfsConfig)
    val hdfsFileOpen = hdfsfileSystem.open(propFilepath)
    prop.load(hdfsFileOpen)

    System.setProperty("java.security.krb5.conf", prop.getProperty(KRB5_CONF))
    System.setProperty("java.security.krb5.realm", prop.getProperty(KRB5_REALM))
    System.setProperty("java.security.krb5.kdc", prop.getProperty(KRB5_KDC))

    val configuration = new Configuration()
    configuration.set("hbase.security.authentication", prop.getProperty(HBASE_AUTH))
    configuration.set("hbase.rest.authentication.kerberos.keytab", prop.getProperty(KEYTAB_FILE))
    configuration.set("hbase.master.keytab.file", prop.getProperty(KEYTAB_FILE))
    configuration.set("hbase.regionserver.keytab.file", prop.getProperty(KEYTAB_FILE))
    configuration.set("hbase.cluster.distributed", prop.getProperty(HBASE_CLUSTER))
    configuration.set("hbase.rpc.protection", prop.getProperty(RPC_AUTH))
    configuration.set("hbase.client.retries.number", prop.getProperty(HBASE_RETRY));
    configuration.set("hbase.regionserver.kerberos.principal", prop.getProperty(KERBEROS_PRINCIPAL))
    configuration.set("hbase.master.kerberos.principal",prop.getProperty(KERBEROS_PRINCIPAL))
    UserGroupInformation.setConfiguration(configuration);
    val loginUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(prop.getProperty(KERBEROS_PRINCIPAL),  prop.getProperty(KEYTAB))
    println("Login user created :>> " + loginUser)

    val MSTDATA_PATH = prop.getProperty(MST_PRIMARY)
    val MSTBCK_PATH = prop.getProperty(MST_BACKUP)
    val TEMPPATH = prop.getProperty(TEMP_PATH)

    //***********************Initial Load**************************
 /*   val MSTDATA_PATH ="/user/datalake/ajangid/HbaseProcessing/Test/MasterData/Primary/"
    val MSTBCK_PATH ="/user/datalake/ajangid/HbaseProcessing/Test/MasterData/Backup/"*/

    var statmst_GG_DF:DataFrame =null;
    val StatmstSchema = StructType(
      List(
        StructField("stat_cstatcode", StringType),
        StructField("stat_cstattype", StringType),
        StructField("stat_cstatgroup", StringType),
        StructField("stat_cstatdesc", StringType)))
    var newCPJ:DataFrame = null;    
    
    loginUser.doAs(new PrivilegedExceptionAction[Unit] {
      override def run: Unit = {
        //creating a configuration object

        //val tableName = "CT_INCREMENTAL_LOAD"
        /* var IL_tableName_ADT : String = null
        var PL_tableName_ADT : String = null
        var IL_tableName_AWBNO : String = null
        var PL_tableName_AWBNO : String = null*/

        val config = HBaseConfiguration.create()
        //      config.clear()
        config.set("hbase.zookeeper.quorum", prop.getProperty(ZKQUORUM))
        config.set("hbase.zookeeper.property.clientPort", prop.getProperty(ZKPORT))
        config.set("hbase.regionserver.compaction.enabled", prop.getProperty(COMPACTION))
        //config.set(TableInputFormat.INPUT_TABLE, IL_tableName)

        //Creating HBaseAdmin object
        val admin = new HBaseAdmin(config)

        /*** Incremental Tables **/

        val tableName_ADT = prop.getProperty(AWBID)
        val IL_tableName_AWBNO = prop.getProperty(IL_AWBNO)
        val PL_tableName_CPS = prop.getProperty(PL_CALLPUS)
        val PL_tableName_AWBNO = prop.getProperty(PL_AWBNO)
        val IL_tableName_COAWBNO = prop.getProperty(IL_COAWBNO)
        val PL_tableName_COAWBNO = prop.getProperty(PL_COAWBNO)
        val BATCH_CNTRL = prop.getProperty(BATCH_CNTL)


        /*** Test Tables **/

       /* val tableName_ADT = "CT_THREAD_AWBID"
        val IL_tableName_AWBNO = "CT_INCREMENTAL_LOAD"
        val PL_tableName_CPS = "CT_THREAD_TOKENNO"
        val PL_tableName_AWBNO = "CT_PERSISTENT_LOAD"
        val IL_tableName_COAWBNO = "CT_THREAD_IL_LOAD_COAWBNO"
        val PL_tableName_COAWBNO = "CT_THREAD_PL_LOAD_COAWBNO"
        val BATCH_CNTRL = "TEST_BATCH_CNTRL"*/

        /*** Initial Load Tables **/

/*        val tableName_ADT = "TEST_AWBID"
        val IL_tableName_AWBNO = "CTDEV.CT_IL_CAWBNO_INITIAL"
        val PL_tableName_CPS = "CTDEV.CT_PL_TOKENNO_INITIAL"
        val PL_tableName_AWBNO = "CTDEV.CT_PL_CAWBNO_INITIAL"
        val IL_tableName_COAWBNO = "CTDEV.CT_IL_COAWBNO_INITIAL"
        val PL_tableName_COAWBNO = "CTDEV.CT_PL_COAWBNO_INITIAL"
        val BATCH_CNTRL = "TEST_BATCH_CNTRL"*/

        config.set(TableInputFormat.INPUT_TABLE, IL_tableName_AWBNO)
        /*println("Enable IL tables")
        admin.enableTable(TableName.valueOf(IL_tableName_AWBNO))*/

        /*Truncate IL tables*/
        truncateHbaseTable(IL_tableName_AWBNO,admin)
        truncateHbaseTable(IL_tableName_COAWBNO,admin)

       /* println("Before disabling IL table")
        admin.disableTable(TableName.valueOf(IL_tableName_AWBNO))
        println("Before truncating IL table")
        admin.truncateTable(TableName.valueOf(IL_tableName_AWBNO), true)
        /*admin.enableTable(TableName.valueOf(IL_tableName_COAWBNO))*/
        println("Before disabling IL COAWBNO table")
        admin.disableTable(TableName.valueOf(IL_tableName_COAWBNO))
        println("Before truncating IL COAWBNO table")
        admin.truncateTable(TableName.valueOf(IL_tableName_COAWBNO), true)*/
        
        var custmstdf: DataFrame = null;
        var packmstDF: DataFrame = null;
        var cmgrpmstDF: DataFrame = null;

        val sdf = new SimpleDateFormat(dateFmt)
        val sdf1 = new SimpleDateFormat(dateFmt1)
        println("Before writing to HBASE:: >>" + sdf.format(new Date()))
        val HbaseSchema = StructType(
          List(
            StructField("PK", StringType),
            StructField("CT_COL", StringType)))

        df_map.foreach { a: (String, DataFrame) =>
          val key = a._1
          val value = a._2
          println(s"key: $key ")

          if (key == "ADT") {

            def IL_catalog_ADT = s"""{
                                |"table":{"namespace":"default", "name":"${tableName_ADT}"},
                                |"rowkey":"PK",
                                |"columns":{
                                |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                                |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                                |}
                                |}""".stripMargin

            println("catalog created :::: " + IL_catalog_ADT)

            println("Before writing to " + key + " IL_catalog_ADT:: >> " + sdf.format(new Date()))

            value.write.options(Map(HBaseTableCatalog.tableCatalog -> IL_catalog_ADT, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

            println("After writing to  " + key + " IL_catalog_ADT:: >> " + sdf.format(new Date()))

          } else if (key == "CPS" || key == "LTS") {

            def catalog_CPS = s"""{
                                 |"table":{"namespace":"default", "name":"${PL_tableName_CPS}"},
                                 |"rowkey":"PK",
                                 |"columns":{
                                 |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                                 |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                                 |}
                                 |}""".stripMargin

            println("catalog created :::: " + catalog_CPS)

            println("Before writing to" + key + " catalog_CPS:: >>" + sdf.format(new Date()))

            value.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog_CPS, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

            println("After writing to " + key + " catalog_CPS:: >>" + sdf.format(new Date()))

          } else if (key == "ALK" ) {

            def IL_catalog_COAWBNO = s"""{
                                 |"table":{"namespace":"default", "name":"${IL_tableName_AWBNO}"},
                                 |"rowkey":"PK",
                                 |"columns":{
                                 |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                                 |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                                 |}
                                 |}""".stripMargin

            println("catalog created :::: " + IL_catalog_COAWBNO)

            println("Before writing to" + key + " IL_catalog_COAWBNO:: >>" + sdf.format(new Date()))

            value.write.options(Map(HBaseTableCatalog.tableCatalog -> IL_catalog_COAWBNO, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

            println("After writing to " + key + " IL_catalog_COAWBNO:: >>" + sdf.format(new Date()))

            /*** Start Changes - Insert AWB to PL_CAWBNO table ***/

            def PL_catalog_CAWBNO = s"""{
                                        |"table":{"namespace":"default", "name":"${PL_tableName_AWBNO}"},
                                        |"rowkey":"PK",
                                        |"columns":{
                                        |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                                        |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                                        |}
                                        |}""".stripMargin

            println("catalog created :::: " + PL_catalog_CAWBNO)

            println("Before writing to" + key + " PL_catalog_CAWBNO:: >>" + sdf.format(new Date()))

            value.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog_CAWBNO, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

            println("After writing to " + key + " PL_catalog_CAWBNO:: >>" + sdf.format(new Date()))

            /*** End Changes - Insert AWB to PL_CAWBNO table ***/

            /*** Start Changes - Insert ALK to COAWBNO table ***/

            def PL_ALK_catalog_COAWBNO = s"""{
                                            |"table":{"namespace":"default", "name":"${PL_tableName_COAWBNO}"},
                                            |"rowkey":"PK",
                                            |"columns":{
                                            |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                                            |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                                            |}
                                            |}""".stripMargin

            println("catalog created :::: " + PL_ALK_catalog_COAWBNO)

            println("Before writing to" + key + " PL_ALK_catalog_COAWBNO:: >>" + sdf.format(new Date()))

            value.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_ALK_catalog_COAWBNO, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

            println("After writing to " + key + " PL_catalog_CAWBNO:: >>" + sdf.format(new Date()))

            /*** End Changes - Insert ALK to COAWBNO table ***/


          } /*else if (key == "SRT") {

            def PL_catalog_COAWBNO = s"""{
                                        |"table":{"namespace":"default", "name":"${IL_tableName_COAWBNO}"},
                                        |"rowkey":"PK",
                                        |"columns":{
                                        |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                                        |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                                        |}
                                        |}""".stripMargin

            println("catalog created :::: " + PL_catalog_COAWBNO)

            println("Before writing to" + key + " PL_catalog_COAWBNO:: >>" + sdf.format(new Date()))

            value.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog_COAWBNO, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

            println("After writing to " + key + " PL_catalog_COAWBNO:: >>" + sdf.format(new Date()))


          } */else if (key == "CPJ") {
            newCPJ = value; 
          } /*else if (key == "SFM") {

            def PL_catalog = s"""{
                                |"table":{"namespace":"default", "name":"${PL_tableName_AWBNO}"},
                                |"rowkey":"PK",
                                |"columns":{
                                |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                                |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                                |}
                                |}""".stripMargin

            println(" SFM catalog created :::: " + PL_catalog)

            println(sdf.format(new Date()))

            value.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

            println("catalog " + key + " written to hbase table:::: " + sdf.format(new Date()))

          }*/
          else {

            def IL_catalog_AWBNO = s"""{
                                |"table":{"namespace":"default", "name":"${IL_tableName_AWBNO}"},
                                |"rowkey":"PK",
                                |"columns":{
                                |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                                |"CT_COL":{"cf":"${key}", "col":"CT_COL", "type":"string"}
                                |}
                                |}""".stripMargin

            println("catalog created :::: " + IL_catalog_AWBNO)

            println("Before writing to " + key + "IL_catalog_AWBNO:: >>" + sdf.format(new Date()))

            value.write.options(Map(HBaseTableCatalog.tableCatalog -> IL_catalog_AWBNO, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

            println("After writing to " + key + " IL_catalog_AWBNO:: >>" + sdf.format(new Date()))

          }

          println("After writing to " + key + " IL_HBASE:: >>" + sdf.format(new Date()))
        }

        // *****************Start Load IL layer Alternate instructions tables to PL layer****************
        config.set(TableInputFormat.INPUT_TABLE, IL_tableName_AWBNO)
        val hBaseRDD2 = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        import sqlContext.implicits._

        val df_hbasetab2 = hBaseRDD2.map(x => {
          (
            Bytes.toString(x._2.getRow),
            Bytes.toString(x._2.getValue(Bytes.toBytes("ALK"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("SRT"), Bytes.toBytes("CT_COL"))))
        }).toDF().withColumnRenamed("_1", "PK").withColumnRenamed("_2", "ALK").withColumnRenamed("_3", "SRT")

        df_hbasetab2.registerTempTable("hbasetab2")

        val ILayer_DF = sqlContext.sql("""SELECT PK ,SPLIT(ALK,'~')[0] AS ALK_COAWBNO, SPLIT(ALK,'~')[1] AS ALK_COORGAREA, SPLIT(ALK,'~')[2] AS ALK_CODSTAREA, SPLIT(ALK,'~')[3] AS ALK_CNAWBNO, SPLIT(ALK,'~')[4] AS ALK_CNORGAREA, SPLIT(ALK,'~')[5] AS ALK_CNDSTAREA, SPLIT(ALK,'~')[6] AS ALK_NAMT, SPLIT(ALK,'~')[7] AS ALK_CPRODCODE, SPLIT(ALK,'~')[8] AS ALK_DDATE, SPLIT(ALK,'~')[9] AS ALK_DFLIGHTDT, SPLIT(ALK,'~')[10] AS ALK_CFLAG, SPLIT(ALK,'~')[11] AS ALK_CPRODTYPE, SPLIT(ALK,'~')[12] AS ALK_CFLIGHTNO, SPLIT(ALK,'~')[13] AS ALK_CLOCCODE, SPLIT(ALK,'~')[14] AS ALK_DDFILEDATE, SPLIT(ALK,'~')[15] AS ALK_OP_TS, SPLIT(ALK,'~')[16] AS ALK_OP_TYPE, SPLIT(ALK,'~')[17] AS ALK_LOADTS,SPLIT(SRT,'~')[0] AS SRT_CAWBNO, SPLIT(SRT,'~')[1] AS SRT_CTYPE, SPLIT(SRT,'~')[2] AS SRT_CEMPLCODE, SPLIT(SRT,'~')[3] AS SRT_CTIMEAREA, SPLIT(SRT,'~')[4] AS SRT_DATTEMPDT, SPLIT(SRT,'~')[5] AS SRT_CEMPLEMAIL, SPLIT(SRT,'~')[6] AS SRT_CEMPLNAME, SPLIT(SRT,'~')[7] AS SRT_CATTENTION, SPLIT(SRT,'~')[8] AS SRT_CNAWBNO, SPLIT(SRT,'~')[9] AS SRT_DENTDATE, SPLIT(SRT,'~')[10] AS SRT_CENTTIME, SPLIT(SRT,'~')[11] AS SRT_CORGAREA, SPLIT(SRT,'~')[12] AS SRT_CDSTAREA, SPLIT(SRT,'~')[13] AS SRT_CPRODCODE, SPLIT(SRT,'~')[14] AS SRT_CCUSTCODE, SPLIT(SRT,'~')[15] AS SRT_CMODE, SPLIT(SRT,'~')[16] AS SRT_CMAILLOC, SPLIT(SRT,'~')[17] AS SRT_CREMARKS, SPLIT(SRT,'~')[18] AS SRT_CCUSTNAME, SPLIT(SRT,'~')[19] AS SRT_CTYP, SPLIT(SRT,'~')[20] AS SRT_BSRYALTINST, SPLIT(SRT,'~')[21] AS SRT_CRECDBY, SPLIT(SRT,'~')[22] AS SRT_CRELATION, SPLIT(SRT,'~')[23] AS SRT_CSTATCODE, SPLIT(SRT,'~')[24] AS SRT_CPUWI, SPLIT(SRT,'~')[25] AS SRT_DIMPORTDT, SPLIT(SRT,'~')[26] AS SRT_OPTS, SPLIT(SRT,'~')[27] AS SRT_OPTYPE, SPLIT(SRT,'~')[28] AS SRT_LOADTS FROM hbasetab2""")
        
        
        ILayer_DF.registerTempTable("incrlayer")

        val srtDF = sqlContext.sql("""SELECT distinct SRT_CAWBNO AS CAWBNO,CONCAT(concat_ws('~',nvl(SRT_DENTDATE,''),nvl(SRT_CREMARKS,'')),":",SRT_OPTYPE) as journey FROM incrlayer where SRT_CAWBNO != 'NA!@#'""")


        srtDF.registerTempTable("sryalt")

        val df_group_test2 = srtDF.groupBy(srtDF("CAWBNO")).agg(sort_array(collect_list(srtDF("journey"))).alias("curr_sralt_journey"))

        val srt_test = srtDF.join(df_group_test2, srtDF("CAWBNO") === df_group_test2("CAWBNO"), "inner").select(srtDF("CAWBNO"), df_group_test2("curr_sralt_journey").alias("sryalt_journey"))

        val sryaltILDF = srt_test.withColumn("sryalt_journey", concat_ws("|", srt_test("sryalt_journey"))).distinct

        sryaltILDF.registerTempTable("sryjourney")
        val ILWithSRYJourney = sqlContext.sql("""select incrlayer.*,sryjourney.sryalt_journey from incrlayer left outer join sryjourney on sryjourney.CAWBNO = incrlayer.PK where SRT_CAWBNO!= 'NA!@#'""")

        ILWithSRYJourney.registerTempTable("ilsrjry")
        //config.set(TableInputFormat.INPUT_TABLE, PL_tableName_COAWBNO)
        config.set(TableInputFormat.INPUT_TABLE, PL_tableName_AWBNO)
        val hBaseRDDPL2 = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        import sqlContext.implicits._

        val df_pl2 = hBaseRDDPL2.map(x => {
          (
            Bytes.toString(x._2.getRow),
            Bytes.toString(x._2.getValue(Bytes.toBytes("SRJ"), Bytes.toBytes("CT_COL"))))
        }).toDF().withColumnRenamed("_1", "PK").withColumnRenamed("_2", "SRJ")

        println("After reading PL data as a dataframe" + sdf.format(new Date()))

        val existing_PL2 = sryaltILDF.join(df_pl2, df_pl2("PK") === sryaltILDF("CAWBNO"), "inner").select(df_pl2("PK").alias("CAWBNO"), df_pl2("SRJ").alias("CT_COL"))

        println("PL Table read completed::: " + sdf.format(new Date()))

        val joinSrtJourney = ILWithSRYJourney.join(existing_PL2, existing_PL2("CAWBNO") === ILWithSRYJourney("PK"), "left").select(col("PK"), col("sryalt_journey").alias("IL_Journey"), col("CT_COL").alias("PL_Journey"))

        joinSrtJourney.registerTempTable("joinsrt")
      
        val finalSrtJourney = sqlContext.sql("""SELECT PK,CASE WHEN PL_Journey != 'NA!@#' OR PL_Journey is not null or  trim(PL_Journey) != "" THEN CONCAT_WS('|',PL_Journey,IL_Journey) ELSE IL_Journey END AS journey from joinsrt""")

        finalSrtJourney.registerTempTable("finalsrt")
        var srtJourney :DataFrame = null;
        var sryalt_DF :DataFrame = null;

         srtJourney = sqlContext.sql("""select NVL(PK,'NA!@#') as PK,NVL(journey,'NA!@#') as CT_COL from finalsrt""")

        sryalt_DF = sqlContext.sql(""" select NVL(TRIM(PK),'NA!@#') as PK,CONCAT_WS('~',NVL(SRT_CAWBNO,'NA!@#'), NVL(SRT_CTYPE,'NA!@#'), NVL(SRT_CEMPLCODE,'NA!@#'), NVL(SRT_CTIMEAREA,'NA!@#'), NVL(SRT_DATTEMPDT,'NA!@#'), NVL(SRT_CEMPLEMAIL,'NA!@#'), NVL(SRT_CEMPLNAME,'NA!@#'), NVL(SRT_CATTENTION,'NA!@#'), NVL(SRT_CNAWBNO,'NA!@#'), NVL(SRT_DENTDATE,'NA!@#'), NVL(SRT_CENTTIME,'NA!@#'), NVL(SRT_CORGAREA,'NA!@#'), NVL(SRT_CDSTAREA,'NA!@#'), NVL(SRT_CPRODCODE,'NA!@#'), NVL(SRT_CCUSTCODE,'NA!@#'), NVL(SRT_CMODE,'NA!@#'), NVL(SRT_CMAILLOC,'NA!@#'), NVL(SRT_CREMARKS,'NA!@#'), NVL(SRT_CCUSTNAME,'NA!@#'), NVL(SRT_CTYP,'NA!@#'), NVL(SRT_BSRYALTINST,'NA!@#'), NVL(SRT_CRECDBY,'NA!@#'), NVL(SRT_CRELATION,'NA!@#'), NVL(SRT_CSTATCODE,'NA!@#'), NVL(SRT_CPUWI,'NA!@#'), NVL(SRT_DIMPORTDT,'NA!@#'), NVL(SRT_OPTS,'NA!@#'), NVL(SRT_OPTYPE,'NA!@#')) AS CT_COL, 'Y' as AFLG from ilsrjry where SRT_CAWBNO!= 'NA!@#'""")

      /*  val awblist_DF = sqlContext.sql("""select NVL(TRIM(PK),'NA!@#') as PK,CONCAT_WS('~', NVL(ALK_COAWBNO,'NA!@#'), NVL(ALK_COORGAREA,'NA!@#'), NVL(ALK_CODSTAREA,'NA!@#'), NVL(ALK_CNAWBNO,'NA!@#'), NVL(ALK_CNORGAREA,'NA!@#'), NVL(ALK_CNDSTAREA,'NA!@#'), NVL(ALK_NAMT,'NA!@#'), NVL(ALK_CPRODCODE,'NA!@#'), NVL(ALK_DDATE,'NA!@#'), NVL(ALK_DFLIGHTDT,'NA!@#'), NVL(ALK_CFLAG,'NA!@#'), NVL(ALK_CPRODTYPE,'NA!@#'), NVL(ALK_CFLIGHTNO,'NA!@#'), NVL(ALK_CLOCCODE,'NA!@#'), NVL(ALK_DDFILEDATE,'NA!@#'), NVL(ALK_OP_TS,'NA!@#'), NVL(ALK_OP_TYPE,'NA!@#')) AS CT_COL from ilsrjry where ALK_COAWBNO!= 'NA!@#'""")*/
/*
        val awblnk_maxts = sqlContext.sql("select max(ALK_LOADTS) AS MAXLTS from ilsrjry where ALK_COAWBNO!= 'NA!@#'")

        val sryalt_maxts = sqlContext.sql("select max(SRT_LOADTS) AS MAXLTS from ilsrjry where SRT_CAWBNO!= 'NA!@#'")

        val max_alt_TS = awblnk_maxts.unionAll(sryalt_maxts).agg(max(col("MAXLTS"))).collect()(0)(0).toString()

        val loadts_DF = sqlContext.sql(s"""select NVL(TRIM(PK),'NA!@#') as PK,'$max_alt_TS'  AS CT_COL from ilsrjry""")*/

//        val pl_map2 = Map("SRJ" -> srtJourney, "SRT" -> sryalt_DF, "ALK" -> awblist_DF, "LTS" -> loadts_DF)

        if(srtJourney == null){
          println("srtJourney is null, creating empty dataframe")
          srtJourney = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)

        }
        if(sryalt_DF == null){
          println("sryalt_DF is null, creating empty dataframe")
          sryalt_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)

        }
          //val pl_map2 = Map("SRJ" -> srtJourney, "SRT" -> sryalt_DF)

          def PL_catalog2 = s"""{
                              |"table":{"namespace":"default", "name":"${PL_tableName_AWBNO}"},
                              |"rowkey":"PK",
                              |"columns":{
                              |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                              |"CT_COL":{"cf":"SRT", "col":"CT_COL", "type":"string"},
                              |"AFLG":{"cf":"SRT", "col":"AFLG", "type":"string"}
                              |}
                              |}""".stripMargin

          println("catalog created :::: " + PL_catalog2)

          println(sdf.format(new Date()))

        sryalt_DF.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog2, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

          println("catalog SRT written to hbase table:::: " + sdf.format(new Date()))

        def PL_COAWBNO_catalog1 = s"""{
                             |"table":{"namespace":"default", "name":"${PL_tableName_COAWBNO}"},
                             |"rowkey":"PK",
                             |"columns":{
                             |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                             |"CT_COL":{"cf":"SRT", "col":"CT_COL", "type":"string"},
                             |"AFLG":{"cf":"SRT", "col":"AFLG", "type":"string"}
                             |}
                             |}""".stripMargin

        println("catalog created :::: " + PL_COAWBNO_catalog1)

        println(sdf.format(new Date()))

        sryalt_DF.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_COAWBNO_catalog1, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

        println("catalog SRT written to hbase table:::: " + sdf.format(new Date()))

        def PL_catalog4 = s"""{
                             |"table":{"namespace":"default", "name":"${PL_tableName_AWBNO}"},
                             |"rowkey":"PK",
                             |"columns":{
                             |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                             |"CT_COL":{"cf":"SRJ", "col":"CT_COL", "type":"string"}
                             |}
                             |}""".stripMargin

        println("catalog created :::: " + PL_catalog4)

        println(sdf.format(new Date()))

        srtJourney.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_catalog4, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

        println("catalog SRJ written to hbase table:::: " + sdf.format(new Date()))

          /** Start Changes - Add SRT and SRJ to COAWBNO table **/

          def PL_COAWBNO_catalog2 = s"""{
                                       |"table":{"namespace":"default", "name":"${PL_tableName_COAWBNO}"},
                                       |"rowkey":"PK",
                                       |"columns":{
                                       |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                                       |"CT_COL":{"cf":"SRJ", "col":"CT_COL", "type":"string"}
                                       |}
                                       |}""".stripMargin

          println("catalog created :::: " + PL_COAWBNO_catalog2)

          println(sdf.format(new Date()))

        srtJourney.write.options(Map(HBaseTableCatalog.tableCatalog -> PL_COAWBNO_catalog2, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

          println("catalog SRJ written to hbase table:::: " + sdf.format(new Date()))
          /** End Changes - Add SRT and SRJ to COAWBNO table **/



        println("after write to PL Hbase" + sdf.format(new Date()))

        //***************Complete Load IL layer Alternate instructions tables to PL layer***************
        
        //*************************Start CALLPUS Journey Creation*****************
    config.set(TableInputFormat.INPUT_TABLE, PL_tableName_CPS)
    

        val hBaseCPRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        val hbaseCPTab = hBaseCPRDD.map(x => {
          (
            Bytes.toString(x._2.getRow),
            Bytes.toString(x._2.getValue(Bytes.toBytes("CPJ"), Bytes.toBytes("CT_COL"))))
        }).toDF().withColumnRenamed("_1", "PK").withColumnRenamed("_2", "CPJ")
        
        println("Existing CP Journey")
        
        if(newCPJ.take(1).length==0){
          newCPJ = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }
        
        println("New CP Journey")
        
 
        val joinCallpusJourney = newCPJ.join(hbaseCPTab, hbaseCPTab("PK") === newCPJ("PK"), "left").select(newCPJ("PK"), newCPJ("CT_COL").alias("IL_Journey"), hbaseCPTab("CPJ").alias("PL_Journey"))

        joinCallpusJourney.registerTempTable("joincallpus")

        val finalCallpusJourney = sqlContext.sql("""SELECT PK,CASE WHEN PL_Journey != 'NA!@#' OR PL_Journey is not null THEN CONCAT_WS('|',PL_Journey,IL_Journey) ELSE IL_Journey END AS journey from joincallpus""")
        finalCallpusJourney.registerTempTable("finalcallpus")
        var CPJourney : DataFrame = null;
         CPJourney = sqlContext.sql("""select NVL(TRIM(PK),'NA!@#') as PK,NVL(journey,'NA!@#') as CT_COL from finalcallpus""")
        if(CPJourney == null){
          println("CPJourney is null, creating empty dataframe")
          CPJourney = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)

        }
        println("CallPus Final Journey")
        
        
        def catalog_CPS = s"""{
                                 |"table":{"namespace":"default", "name":"${PL_tableName_CPS}"},
                                 |"rowkey":"PK",
                                 |"columns":{
                                 |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                                 |"CT_COL":{"cf":"CPJ", "col":"CT_COL", "type":"string"}
                                 |}
                                 |}""".stripMargin

            println("catalog created :::: " + catalog_CPS)

            println("Before writing to CPJ catalog_CPS:: >>" + sdf.format(new Date()))

            CPJourney.write.options(Map(HBaseTableCatalog.tableCatalog -> catalog_CPS, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()
            println("After writing to CPJ catalog_CPS:: >>" + sdf.format(new Date()))
     //*************************End CALLPUS Journey Creation*****************
            
        //*******************Process IL Layer CAWBNO Table***********

        config.set(TableInputFormat.INPUT_TABLE, IL_tableName_AWBNO)
        val hBaseRDD = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        import sqlContext.implicits._

        val df_hbasetab = hBaseRDD.map(x => {
          (
            Bytes.toString(x._2.getRow),
            Bytes.toString(x._2.getValue(Bytes.toBytes("IBS"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("OBS"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("ABM"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("SLT"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("MDP"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("MOD"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("SFM"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("PDJ"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("ALK"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("SRT"), Bytes.toBytes("CT_COL"))),
            Bytes.toString(x._2.getValue(Bytes.toBytes("SFJ"), Bytes.toBytes("CT_COL")))
            )
        }).toDF().withColumnRenamed("_1", "PK").withColumnRenamed("_2", "IBS").withColumnRenamed("_3", "OBS").withColumnRenamed("_4", "ABS").withColumnRenamed("_5", "SLT").withColumnRenamed("_6", "MDP").withColumnRenamed("_7", "MOD").withColumnRenamed("_8", "SFM").withColumnRenamed("_9", "PDJ").withColumnRenamed("_10", "ALK").withColumnRenamed("_11", "SRT").withColumnRenamed("_12", "SFJ")

        df_hbasetab.registerTempTable("hbasetab")

        val IncrLayer_DF = sqlContext.sql("""SELECT PK , SPLIT(OBS,'~')[0] AS OBS_CAWBNO, SPLIT(OBS,'~')[1] AS OBS_NSTATUSID, SPLIT(OBS,'~')[2] AS OBS_NOPERATIONID, SPLIT(OBS,'~')[3] AS OBS_CPRODCODE, SPLIT(OBS,'~')[4] AS OBS_CORGAREA, SPLIT(OBS,'~')[5] AS OBS_CDSTAREA, SPLIT(OBS,'~')[6] AS OBS_CMPSNO, SPLIT(OBS,'~')[7] AS OBS_CSTATCODE, SPLIT(OBS,'~')[8] AS OBS_CSTATTYPE, SPLIT(OBS,'~')[9] AS OBS_DSTATDATE, SPLIT(OBS,'~')[10] AS OBS_CREMARKS, SPLIT(OBS,'~')[11] AS OBS_CLOCCODE, SPLIT(OBS,'~')[12] AS OBS_CEMPLCODE, SPLIT(OBS,'~')[13] AS OBS_CFILENAME, SPLIT(OBS,'~')[14] AS OBS_SEDPTRNX, SPLIT(OBS,'~')[15] AS OBS_DEDPTRNX, SPLIT(OBS,'~')[16] AS OBS_CCNO, SPLIT(OBS,'~')[17] AS OBS_DRUNDATE, SPLIT(OBS,'~')[18] AS OBS_DLASTMODIFIEDTS, SPLIT(OBS,'~')[19] AS OBS_OP_TS, SPLIT(OBS,'~')[20] AS OBS_OP_TYPE, SPLIT(OBS,'~')[21] AS OBS_LOADTS, SPLIT(IBS,'~')[0] AS IBS_CAWBNO, SPLIT(IBS,'~')[1] AS IBS_NSTATUSID, SPLIT(IBS,'~')[2] AS IBS_NOPERATIONID, SPLIT(IBS,'~')[3] AS IBS_CMPSNO, SPLIT(IBS,'~')[4] AS IBS_CSTATCODE, SPLIT(IBS,'~')[5] AS IBS_CSTATTYPE, SPLIT(IBS,'~')[6] AS IBS_DSTATDATE, SPLIT(IBS,'~')[7] AS IBS_CREMARKS, SPLIT(IBS,'~')[8] AS IBS_CLOCCODE, SPLIT(IBS,'~')[9] AS IBS_CEMPLCODE, SPLIT(IBS,'~')[10] AS IBS_CFILENAME, SPLIT(IBS,'~')[11] AS IBS_SEDPTRNX, SPLIT(IBS,'~')[12] AS IBS_DEDPTRNX, SPLIT(IBS,'~')[13] AS IBS_DFILENAME, SPLIT(IBS,'~')[14] AS IBS_CPRODCODE, SPLIT(IBS,'~')[15] AS IBS_CORGAREA, SPLIT(IBS,'~')[16] AS IBS_CDSTAREA, SPLIT(IBS,'~')[17] AS IBS_DENTDATE, SPLIT(IBS,'~')[18] AS IBS_DLASTMODIFIEDTS, SPLIT(IBS,'~')[19] AS IBS_OP_TS, SPLIT(IBS,'~')[20] AS IBS_OP_TYPE, SPLIT(IBS,'~')[21] AS IBS_LOADTS, SPLIT(ABS,'~')[0] AS ABS_CAWBNO, SPLIT(ABS,'~')[1] AS ABS_NAWBID, SPLIT(ABS,'~')[2] AS ABS_CPRODCODE, SPLIT(ABS,'~')[3] AS ABS_CPRODTYPE, SPLIT(ABS,'~')[4] AS ABS_CORGAREA, SPLIT(ABS,'~')[5] AS ABS_CDSTAREA, SPLIT(ABS,'~')[6] AS ABS_CBATCHNO, SPLIT(ABS,'~')[7] AS ABS_NPCS, SPLIT(ABS,'~')[8] AS ABS_DSHIPDATE, SPLIT(ABS,'~')[9] AS ABS_DPUDATE, SPLIT(ABS,'~')[10] AS ABS_NTOKENNO, SPLIT(ABS,'~')[11] AS ABS_CPUWI, SPLIT(ABS,'~')[12] AS ABS_NDIMWEIGHT, SPLIT(ABS,'~')[13] AS ABS_CCUSTCODE, SPLIT(ABS,'~')[14] AS ABS_NCSHMEMNO, SPLIT(ABS,'~')[15] AS ABS_CCMDTYCODE, SPLIT(ABS,'~')[16] AS ABS_NCOLAMT, SPLIT(ABS,'~')[17] AS ABS_NAMT, SPLIT(ABS,'~')[18] AS ABS_CTRNCODE, SPLIT(ABS,'~')[19] AS ABS_CSPLINST, SPLIT(ABS,'~')[20] AS ABS_CPACKTYPE, SPLIT(ABS,'~')[21] AS ABS_CODA, SPLIT(ABS,'~')[22] AS ABS_CMODE, SPLIT(ABS,'~')[23] AS ABS_CGSACODE, SPLIT(ABS,'~')[24] AS ABS_CFODCODFLG, SPLIT(ABS,'~')[25] AS ABS_CFOCCODE, SPLIT(ABS,'~')[26] AS ABS_CBTPCODE, SPLIT(ABS,'~')[27] AS ABS_CBTPAREA, SPLIT(ABS,'~')[28] AS ABS_CBILLNO, SPLIT(ABS,'~')[29] AS ABS_BREFNO, SPLIT(ABS,'~')[30] AS ABS_BDUTY, SPLIT(ABS,'~')[31] AS ABS_CSECTYCK, SPLIT(ABS,'~')[32] AS ABS_NPARRENTCONTID, SPLIT(ABS,'~')[33] AS ABS_BBILLCNEE, SPLIT(ABS,'~')[34] AS ABS_CPUEMPCODE, SPLIT(ABS,'~')[35] AS ABS_BSTAMDART, SPLIT(ABS,'~')[36] AS ABS_CORGSCRCD, SPLIT(ABS,'~')[37] AS ABS_CDSTSCRCD, SPLIT(ABS,'~')[38] AS ABS_BPRIORITY, SPLIT(ABS,'~')[39] AS ABS_CSUBCODE, SPLIT(ABS,'~')[40] AS ABS_NCURLOOKUPID, SPLIT(ABS,'~')[41] AS ABS_CPUTIME, SPLIT(ABS,'~')[42] AS ABS_CFLFM, SPLIT(ABS,'~')[43] AS ABS_CDHLACCTNO, SPLIT(ABS,'~')[44] AS ABS_CRTOAREA, SPLIT(ABS,'~')[45] AS ABS_CRTOCODE, SPLIT(ABS,'~')[46] AS ABS_CEMAILID, SPLIT(ABS,'~')[47] AS ABS_NWEIGHT, SPLIT(ABS,'~')[48] AS ABS_NOCTROI, SPLIT(ABS,'~')[49] AS ABS_NDUTY, SPLIT(ABS,'~')[50] AS ABS_NSMARTBOXTYPEA, SPLIT(ABS,'~')[51] AS ABS_NSMARTBOXTYPEB, SPLIT(ABS,'~')[52] AS ABS_NDCCOUNT, SPLIT(ABS,'~')[53] AS ABS_CBILLCAC, SPLIT(ABS,'~')[54] AS ABS_BCRCRDREF, SPLIT(ABS,'~')[55] AS ABS_NDCRECD, SPLIT(ABS,'~')[56] AS ABS_CDLVPURTCD, SPLIT(ABS,'~')[57] AS ABS_CACCURACY, SPLIT(ABS,'~')[58] AS ABS_CDLCODE, SPLIT(ABS,'~')[59] AS ABS_CKGPOUND, SPLIT(ABS,'~')[60] AS ABS_CMLOCCODE, SPLIT(ABS,'~')[61] AS ABS_CADECODE, SPLIT(ABS,'~')[62] AS ABS_DINSCANDT, SPLIT(ABS,'~')[63] AS ABS_CINSCANLOC, SPLIT(ABS,'~')[64] AS ABS_BEDPTRNX, SPLIT(ABS,'~')[65] AS ABS_BCRCRDPAY, SPLIT(ABS,'~')[66] AS ABS_BIATADTL, SPLIT(ABS,'~')[67] AS ABS_CDOCATTACH, SPLIT(ABS,'~')[68] AS ABS_CEMPLCODE, SPLIT(ABS,'~')[69] AS ABS_NOPCS, SPLIT(ABS,'~')[70] AS ABS_CMUSTGOCRG, SPLIT(ABS,'~')[71] AS ABS_CODOCATTAC, SPLIT(ABS,'~')[72] AS ABS_CDATAENTRYLOC, SPLIT(ABS,'~')[73] AS ABS_BSHPCRCRDREF, SPLIT(ABS,'~')[74] AS ABS_DEPTDTDLV, SPLIT(ABS,'~')[75] AS ABS_BDETAIN, SPLIT(ABS,'~')[76] AS ABS_CISOVERAGE, SPLIT(ABS,'~')[77] AS ABS_CDHLFLAG, SPLIT(ABS,'~')[78] AS ABS_DDEMUDT, SPLIT(ABS,'~')[79] AS ABS_NDEMUAMT, SPLIT(ABS,'~')[80] AS ABS_CDEMUCODE, SPLIT(ABS,'~')[81] AS ABS_CDEMULOCCODE, SPLIT(ABS,'~')[82] AS ABS_DSTATUSDT, SPLIT(ABS,'~')[83] AS ABS_BCSBPRINTED, SPLIT(ABS,'~')[84] AS ABS_DDATAENTRYDT, SPLIT(ABS,'~')[85] AS ABS_DBATCHDT, SPLIT(ABS,'~')[86] AS ABS_CASTATTYPE, SPLIT(ABS,'~')[87] AS ABS_CASTATCODE, SPLIT(ABS,'~')[88] AS ABS_CSTATEMPLCODE, SPLIT(ABS,'~')[89] AS ABS_CAREMARKS, SPLIT(ABS,'~')[90] AS ABS_CAPTCODE, SPLIT(ABS,'~')[91] AS ABS_BPWPALLETIZED, SPLIT(ABS,'~')[92] AS ABS_CPRINTMODE, SPLIT(ABS,'~')[93] AS ABS_CPROMOCODE, SPLIT(ABS,'~')[94] AS ABS_CRTOIMTLY, SPLIT(ABS,'~')[95] AS ABS_BDGSHIPMENT, SPLIT(ABS,'~')[96] AS ABS_ISPWREC, SPLIT(ABS,'~')[97] AS ABS_CPREFTM, SPLIT(ABS,'~')[98] AS ABS_CREVPU, SPLIT(ABS,'~')[99] AS ABS_CPSCODE, SPLIT(ABS,'~')[100] AS ABS_CFRDAWBNO, SPLIT(ABS,'~')[101] AS ABS_CRFDCOMPNM, SPLIT(ABS,'~')[102] AS ABS_CREFNO2, SPLIT(ABS,'~')[103] AS ABS_CREFNO3, SPLIT(ABS,'~')[104] AS ABS_CPUMODE, SPLIT(ABS,'~')[105] AS ABS_CPUTYPE, SPLIT(ABS,'~')[106] AS ABS_NITEMCNT, SPLIT(ABS,'~')[107] AS ABS_BPARTIALPU, SPLIT(ABS,'~')[108] AS ABS_NPAYCASH, SPLIT(ABS,'~')[109] AS ABS_NPUTMSLOT, SPLIT(ABS,'~')[110] AS ABS_CMANIFSTNO, SPLIT(ABS,'~')[111] AS ABS_COFFCLTIME, SPLIT(ABS,'~')[112] AS ABS_NDEFERREDDELIVERYDAYS, SPLIT(ABS,'~')[113] AS ABS_DCUSTEDD, SPLIT(ABS,'~')[114] AS ABS_CISDDN, SPLIT(ABS,'~')[115] AS ABS_CACTDELLOC, SPLIT(ABS,'~')[116] AS ABS_CGSTNO, SPLIT(ABS,'~')[117] AS ABS_CVEHICLENO, SPLIT(ABS,'~')[118] AS ABS_CEXCHAWB, SPLIT(ABS,'~')[119] AS ABS_DPREFDATE, SPLIT(ABS,'~')[120] AS ABS_CPREFTIME, SPLIT(ABS,'~')[121] AS ABS_NDISTANCE, SPLIT(ABS,'~')[122] AS ABS_DCUSTPUDT, SPLIT(ABS,'~')[123] AS ABS_CCUSTPUTM, SPLIT(ABS,'~')[124] AS ABS_CAVAILTIME, SPLIT(ABS,'~')[125] AS ABS_CAVAILDAYS, SPLIT(ABS,'~')[126] AS ABS_COTPTYPE, SPLIT(ABS,'~')[127] AS ABS_NOTPNO, SPLIT(ABS,'~')[128] AS ABS_CPACKAGINGID, SPLIT(ABS,'~')[129] AS ABS_CCCODE, SPLIT(ABS,'~')[130] AS ABS_OP_TS, SPLIT(ABS,'~')[131] AS ABS_OP_TYPE, SPLIT(ABS,'~')[132] AS ABS_LOADTS, SPLIT(SLT,'~')[0] AS SLT_CAWBNO, SPLIT(SLT,'~')[1] AS SLT_CBATCHNO, SPLIT(SLT,'~')[2] AS SLT_CBATORG, SPLIT(SLT,'~')[3] AS SLT_CPRODCODE, SPLIT(SLT,'~')[4] AS SLT_CORGAREA, SPLIT(SLT,'~')[5] AS SLT_CORGSCRCD, SPLIT(SLT,'~')[6] AS SLT_CMAWBNO, SPLIT(SLT,'~')[7] AS SLT_CMLOCCODE, SPLIT(SLT,'~')[8] AS SLT_CDSTAREA, SPLIT(SLT,'~')[9] AS SLT_CDSTSCRCD, SPLIT(SLT,'~')[10] AS SLT_CMODE, SPLIT(SLT,'~')[11] AS SLT_CPRODTYPE, SPLIT(SLT,'~')[12] AS SLT_CTRNCODE, SPLIT(SLT,'~')[13] AS SLT_CFOCCODE, SPLIT(SLT,'~')[14] AS SLT_BPRIORITY, SPLIT(SLT,'~')[15] AS SLT_CCUSTCODE, SPLIT(SLT,'~')[16] AS SLT_CSENDER, SPLIT(SLT,'~')[17] AS SLT_CCNEECODE, SPLIT(SLT,'~')[18] AS SLT_CATTENTION, SPLIT(SLT,'~')[19] AS SLT_NPCS, SPLIT(SLT,'~')[20] AS SLT_NWEIGHT, SPLIT(SLT,'~')[21] AS SLT_NACTWGT, SPLIT(SLT,'~')[22] AS SLT_CCMDTYCODE, SPLIT(SLT,'~')[23] AS SLT_NTOKENNO, SPLIT(SLT,'~')[24] AS SLT_NCSHMEMNO, SPLIT(SLT,'~')[25] AS SLT_NAMT, SPLIT(SLT,'~')[26] AS SLT_CBILLNO, SPLIT(SLT,'~')[27] AS SLT_BBILLCNEE, SPLIT(SLT,'~')[28] AS SLT_CBILLCAC, SPLIT(SLT,'~')[29] AS SLT_CPACKTYPE, SPLIT(SLT,'~')[30] AS SLT_DSHIPDATE, SPLIT(SLT,'~')[31] AS SLT_DPUDATE, SPLIT(SLT,'~')[32] AS SLT_CPUTIME, SPLIT(SLT,'~')[33] AS SLT_CPUEMPLCD, SPLIT(SLT,'~')[34] AS SLT_CPUWI, SPLIT(SLT,'~')[35] AS SLT_DEPTDTDLV, SPLIT(SLT,'~')[36] AS SLT_CODA, SPLIT(SLT,'~')[37] AS SLT_CFLIGHTNO, SPLIT(SLT,'~')[38] AS SLT_DFLIGHTDT, SPLIT(SLT,'~')[39] AS SLT_CKGPOUND, SPLIT(SLT,'~')[40] AS SLT_BLINKED, SPLIT(SLT,'~')[41] AS SLT_CADECODE, SPLIT(SLT,'~')[42] AS SLT_CCRCRDREF, SPLIT(SLT,'~')[43] AS SLT_NDIML, SPLIT(SLT,'~')[44] AS SLT_NDIMB, SPLIT(SLT,'~')[45] AS SLT_NDIMH, SPLIT(SLT,'~')[46] AS SLT_NSLABWGT, SPLIT(SLT,'~')[47] AS SLT_NASSDVALUE, SPLIT(SLT,'~')[48] AS SLT_CDOCNO, SPLIT(SLT,'~')[49] AS SLT_DDOCDATE, SPLIT(SLT,'~')[50] AS SLT_CPAYTYPE, SPLIT(SLT,'~')[51] AS SLT_NAMOUNT, SPLIT(SLT,'~')[52] AS SLT_CINVNO, SPLIT(SLT,'~')[53] AS SLT_DINVDATE, SPLIT(SLT,'~')[54] AS SLT_NOTHCHRGS, SPLIT(SLT,'~')[55] AS SLT_NCDUTYPC, SPLIT(SLT,'~')[56] AS SLT_CCRCARDNO, SPLIT(SLT,'~')[57] AS SLT_CCARDCODE, SPLIT(SLT,'~')[58] AS SLT_CCARDHOLD, SPLIT(SLT,'~')[59] AS SLT_DVALIDUPTO, SPLIT(SLT,'~')[60] AS SLT_CCUSTNAME, SPLIT(SLT,'~')[61] AS SLT_CCUSTADR1, SPLIT(SLT,'~')[62] AS SLT_CCUSTADR2, SPLIT(SLT,'~')[63] AS SLT_CCUSTADR3, SPLIT(SLT,'~')[64] AS SLT_CCUSTPIN, SPLIT(SLT,'~')[65] AS SLT_CCUSTTEL, SPLIT(SLT,'~')[66] AS SLT_CCUSTFAX, SPLIT(SLT,'~')[67] AS SLT_CCNEENAME, SPLIT(SLT,'~')[68] AS SLT_CCNEEADR1, SPLIT(SLT,'~')[69] AS SLT_CCNEEADR2, SPLIT(SLT,'~')[70] AS SLT_CCNEEADR3, SPLIT(SLT,'~')[71] AS SLT_CCNEEPIN, SPLIT(SLT,'~')[72] AS SLT_CCNEETEL, SPLIT(SLT,'~')[73] AS SLT_CCNEEFAX, SPLIT(SLT,'~')[74] AS SLT_BCHECKLST, SPLIT(SLT,'~')[75] AS SLT_CSPLINST, SPLIT(SLT,'~')[76] AS SLT_CPRODDESC, SPLIT(SLT,'~')[77] AS SLT_DBATCHDT, SPLIT(SLT,'~')[78] AS SLT_NOCTROI, SPLIT(SLT,'~')[79] AS SLT_CCLECTYPE, SPLIT(SLT,'~')[80] AS SLT_NDCLRDVAL, SPLIT(SLT,'~')[81] AS SLT_BSTAMPDART, SPLIT(SLT,'~')[82] AS SLT_CCMDTYDESC, SPLIT(SLT,'~')[83] AS SLT_CCALTNAME, SPLIT(SLT,'~')[84] AS SLT_CALTATTN, SPLIT(SLT,'~')[85] AS SLT_CCALTADR1, SPLIT(SLT,'~')[86] AS SLT_CCALTADR2, SPLIT(SLT,'~')[87] AS SLT_CCALTADR3, SPLIT(SLT,'~')[88] AS SLT_CCALTPIN, SPLIT(SLT,'~')[89] AS SLT_CCALTTEL, SPLIT(SLT,'~')[90] AS SLT_CCALTFAX, SPLIT(SLT,'~')[91] AS SLT_CCNEEMOB, SPLIT(SLT,'~')[92] AS SLT_CCALTMOB, SPLIT(SLT,'~')[93] AS SLT_NCOLAMT, SPLIT(SLT,'~')[94] AS SLT_CFODCODFLG, SPLIT(SLT,'~')[95] AS SLT_CSUBCODE, SPLIT(SLT,'~')[96] AS SLT_CCTMNO, SPLIT(SLT,'~')[97] AS SLT_BDOXATCHD, SPLIT(SLT,'~')[98] AS SLT_CMRKSNOS1, SPLIT(SLT,'~')[99] AS SLT_CMRKSNOS2, SPLIT(SLT,'~')[100] AS SLT_CMRKSNOS3, SPLIT(SLT,'~')[101] AS SLT_CDIMEN1, SPLIT(SLT,'~')[102] AS SLT_CDIMEN2, SPLIT(SLT,'~')[103] AS SLT_CDIMEN3, SPLIT(SLT,'~')[104] AS SLT_NCHRGWT, SPLIT(SLT,'~')[105] AS SLT_NCOMVAL, SPLIT(SLT,'~')[106] AS SLT_NFREIGHT, SPLIT(SLT,'~')[107] AS SLT_NVALCHGS, SPLIT(SLT,'~')[108] AS SLT_NAWBFEE, SPLIT(SLT,'~')[109] AS SLT_CAWBFEE, SPLIT(SLT,'~')[110] AS SLT_NSTATCHG, SPLIT(SLT,'~')[111] AS SLT_CSTATCHG, SPLIT(SLT,'~')[112] AS SLT_NCARTCHG, SPLIT(SLT,'~')[113] AS SLT_CCARTCHG, SPLIT(SLT,'~')[114] AS SLT_NREGTCHG, SPLIT(SLT,'~')[115] AS SLT_CREGTCHG, SPLIT(SLT,'~')[116] AS SLT_NMISCCHG1, SPLIT(SLT,'~')[117] AS SLT_CMISCCHG1, SPLIT(SLT,'~')[118] AS SLT_NMISCCHG2, SPLIT(SLT,'~')[119] AS SLT_CMISCCHG2, SPLIT(SLT,'~')[120] AS SLT_NCHGCOLCT, SPLIT(SLT,'~')[121] AS SLT_COCTRCPTNO, SPLIT(SLT,'~')[122] AS SLT_CEMAILID, SPLIT(SLT,'~')[123] AS SLT_CBTPAREA, SPLIT(SLT,'~')[124] AS SLT_CBTPCODE, SPLIT(SLT,'~')[125] AS SLT_CCODFAVOUR, SPLIT(SLT,'~')[126] AS SLT_CCODPAYBLE, SPLIT(SLT,'~')[127] AS SLT_CFOVTYPE, SPLIT(SLT,'~')[128] AS SLT_NADDISRCHG, SPLIT(SLT,'~')[129] AS SLT_NADDOSRCHG, SPLIT(SLT,'~')[130] AS SLT_NDOCCHRG, SPLIT(SLT,'~')[131] AS SLT_NDCCHRG, SPLIT(SLT,'~')[132] AS SLT_NFODCHRG, SPLIT(SLT,'~')[133] AS SLT_NRISKHCHGS, SPLIT(SLT,'~')[134] AS SLT_NODACHRG, SPLIT(SLT,'~')[135] AS SLT_CGSACODE, SPLIT(SLT,'~')[136] AS SLT_NFSAMT, SPLIT(SLT,'~')[137] AS SLT_CDHLFLAG, SPLIT(SLT,'~')[138] AS SLT_NDODAMT, SPLIT(SLT,'~')[139] AS SLT_CLOCCODE, SPLIT(SLT,'~')[140] AS SLT_CILLOC, SPLIT(SLT,'~')[141] AS SLT_CDLCODE, SPLIT(SLT,'~')[142] AS SLT_NDIWT, SPLIT(SLT,'~')[143] AS SLT_CCUSTMOB, SPLIT(SLT,'~')[144] AS SLT_CADNLCRCRDREF, SPLIT(SLT,'~')[145] AS SLT_DUPLDATE, SPLIT(SLT,'~')[146] AS SLT_CREGCUSTNAME, SPLIT(SLT,'~')[147] AS SLT_CREGCUSTADR1, SPLIT(SLT,'~')[148] AS SLT_CREGCUSTADR2, SPLIT(SLT,'~')[149] AS SLT_CREGCUSTADR3, SPLIT(SLT,'~')[150] AS SLT_CREGCUSTPIN, SPLIT(SLT,'~')[151] AS SLT_CREGCUSTTEL, SPLIT(SLT,'~')[152] AS SLT_CRTOIMDTLY, SPLIT(SLT,'~')[153] AS SLT_CPREFTM, SPLIT(SLT,'~')[154] AS SLT_CREVPU, SPLIT(SLT,'~')[155] AS SLT_CPSCODE, SPLIT(SLT,'~')[156] AS SLT_DDWNLDDATE, SPLIT(SLT,'~')[157] AS SLT_CFRDAWBNO, SPLIT(SLT,'~')[158] AS SLT_CRFDCOMPNM, SPLIT(SLT,'~')[159] AS SLT_CREFNO2, SPLIT(SLT,'~')[160] AS SLT_CREFNO3, SPLIT(SLT,'~')[161] AS SLT_CPUMODE, SPLIT(SLT,'~')[162] AS SLT_CPUTYPE, SPLIT(SLT,'~')[163] AS SLT_NITEMCNT, SPLIT(SLT,'~')[164] AS SLT_BPARTIALPU, SPLIT(SLT,'~')[165] AS SLT_NPAYCASH, SPLIT(SLT,'~')[166] AS SLT_NPUTMSLOT, SPLIT(SLT,'~')[167] AS SLT_CCNEEMAIL, SPLIT(SLT,'~')[168] AS SLT_CRTOCONTNM, SPLIT(SLT,'~')[169] AS SLT_CRTOADR1, SPLIT(SLT,'~')[170] AS SLT_CRTOADR2, SPLIT(SLT,'~')[171] AS SLT_CRTOADR3, SPLIT(SLT,'~')[172] AS SLT_CRTOPIN, SPLIT(SLT,'~')[173] AS SLT_CRTOTEL, SPLIT(SLT,'~')[174] AS SLT_CRTOMOB, SPLIT(SLT,'~')[175] AS SLT_CMANIFSTNO, SPLIT(SLT,'~')[176] AS SLT_CRTOLAT, SPLIT(SLT,'~')[177] AS SLT_CRTOLON, SPLIT(SLT,'~')[178] AS SLT_CRTOADRDT, SPLIT(SLT,'~')[179] AS SLT_CCUSTLAT, SPLIT(SLT,'~')[180] AS SLT_CCUSTLON, SPLIT(SLT,'~')[181] AS SLT_CCUSTADRDT, SPLIT(SLT,'~')[182] AS SLT_CCNEELAT, SPLIT(SLT,'~')[183] AS SLT_CCNEELON, SPLIT(SLT,'~')[184] AS SLT_CCNEEADRDT, SPLIT(SLT,'~')[185] AS SLT_CCALTLAT, SPLIT(SLT,'~')[186] AS SLT_CCALTLON, SPLIT(SLT,'~')[187] AS SLT_CCALTADRDT, SPLIT(SLT,'~')[188] AS SLT_CREGCUSTLAT, SPLIT(SLT,'~')[189] AS SLT_CREGCUSTLON, SPLIT(SLT,'~')[190] AS SLT_CREGCUSTADRDT, SPLIT(SLT,'~')[191] AS SLT_COFFCLTIME, SPLIT(SLT,'~')[192] AS SLT_NDEFERREDDELIVERYDAYS, SPLIT(SLT,'~')[193] AS SLT_CFLFM, SPLIT(SLT,'~')[194] AS SLT_CCNTRYCODE, SPLIT(SLT,'~')[195] AS SLT_CSTATECODE, SPLIT(SLT,'~')[196] AS SLT_DCUSTEDD, SPLIT(SLT,'~')[197] AS SLT_CISDDN, SPLIT(SLT,'~')[198] AS SLT_CACTDELLOC, SPLIT(SLT,'~')[199] AS SLT_CGSTNO, SPLIT(SLT,'~')[200] AS SLT_CCUSTGSTNO, SPLIT(SLT,'~')[201] AS SLT_CCNEEGSTNO, SPLIT(SLT,'~')[202] AS SLT_CEXCHAWB, SPLIT(SLT,'~')[203] AS SLT_CCNEEFADD, SPLIT(SLT,'~')[204] AS SLT_DCUSTPUDT, SPLIT(SLT,'~')[205] AS SLT_CCUSTPUTM, SPLIT(SLT,'~')[206] AS SLT_CCONMOBMASK, SPLIT(SLT,'~')[207] AS SLT_CADDRESSTYPE, SPLIT(SLT,'~')[208] AS SLT_CAVAILTIME, SPLIT(SLT,'~')[209] AS SLT_CAVAILDAYS, SPLIT(SLT,'~')[210] AS SLT_CCSTMOBMAS, SPLIT(SLT,'~')[211] AS SLT_CRTOMOBMAS, SPLIT(SLT,'~')[212] AS SLT_COTPTYPE, SPLIT(SLT,'~')[213] AS SLT_NOTPNO, SPLIT(SLT,'~')[214] AS SLT_CINCOTERMS, SPLIT(SLT,'~')[215] AS SLT_CPURTCD, SPLIT(SLT,'~')[216] AS SLT_CPULOC, SPLIT(SLT,'~')[217] AS SLT_CCOMPGRP ,SPLIT(SLT,'~')[218] AS SLT_CPACKAGINGID, SPLIT(SLT,'~')[219] AS SLT_OP_TS, SPLIT(SLT,'~')[220] AS SLT_OP_TYPE,SPLIT(SLT,'~')[221] AS SLT_LOADTS, SPLIT(MDP,'~')[0] AS MDP_CAWBNO, SPLIT(MDP,'~')[1] AS MDP_CPICKUPID, SPLIT(MDP,'~')[2] AS MDP_CPRODCODE, SPLIT(MDP,'~')[3] AS MDP_CCUSTCODE, SPLIT(MDP,'~')[4] AS MDP_CORGAREA, SPLIT(MDP,'~')[5] AS MDP_CORGSCRCD, SPLIT(MDP,'~')[6] AS MDP_CDSTAREA, SPLIT(MDP,'~')[7] AS MDP_CDSTSCRCD, SPLIT(MDP,'~')[8] AS MDP_CSUBCODE, SPLIT(MDP,'~')[9] AS MDP_CSTATCODE, SPLIT(MDP,'~')[10] AS MDP_DSTATUSDATE, SPLIT(MDP,'~')[11] AS MDP_CMLOCCODE, SPLIT(MDP,'~')[12] AS MDP_CEMPLCODE, SPLIT(MDP,'~')[13] AS MDP_CDEVICENO, SPLIT(MDP,'~')[14] AS MDP_CSIMNO, SPLIT(MDP,'~')[15] AS MDP_CGPSLAT, SPLIT(MDP,'~')[16] AS MDP_CGPSLON, SPLIT(MDP,'~')[17] AS MDP_CGPSTIME, SPLIT(MDP,'~')[18] AS MDP_CGPSSATCNT, SPLIT(MDP,'~')[19] AS MDP_DUPLDDT, SPLIT(MDP,'~')[20] AS MDP_DSYNCDATE, SPLIT(MDP,'~')[21] AS MDP_CSTATUS, SPLIT(MDP,'~')[22] AS MDP_CPURTCODE, SPLIT(MDP,'~')[23] AS MDP_CISPARTIALPICKUP, SPLIT(MDP,'~')[24] AS MDP_CREASONOFPICKUPREJECTION, SPLIT(MDP,'~')[25] AS MDP_CSTATUSTYPE, SPLIT(MDP,'~')[26] AS MDP_CISPICKUPCANCELLED, SPLIT(MDP,'~')[27] AS MDP_NPUTMSLOT, SPLIT(MDP,'~')[28] AS MDP_CTYPE, SPLIT(MDP,'~')[29] AS MDP_DNPUDATE, SPLIT(MDP,'~')[30] AS MDP_CMDPICKUPDETID, SPLIT(MDP,'~')[31] AS MDP_CREMARKS, SPLIT(MDP,'~')[32] AS MDP_OP_TS, SPLIT(MDP,'~')[33] AS MDP_OP_TYPE,SPLIT(MDP,'~')[34] AS MDP_LOADTS,SPLIT(MOD,'~')[0] AS MOD_CAWBNO, SPLIT(MOD,'~')[1] AS MOD_CMPODID, SPLIT(MOD,'~')[2] AS MOD_CMPODDETID, SPLIT(MOD,'~')[3] AS MOD_CPRODCODE, SPLIT(MOD,'~')[4] AS MOD_CORGAREA, SPLIT(MOD,'~')[5] AS MOD_CDSTAREA, SPLIT(MOD,'~')[6] AS MOD_CSTATTYPE, SPLIT(MOD,'~')[7] AS MOD_CSTATCODE, SPLIT(MOD,'~')[8] AS MOD_DSTATDATE, SPLIT(MOD,'~')[9] AS MOD_DSTATTIME, SPLIT(MOD,'~')[10] AS MOD_CEMPLCODE, SPLIT(MOD,'~')[11] AS MOD_CRECDBY, SPLIT(MOD,'~')[12] AS MOD_CDSTSCRCD, SPLIT(MOD,'~')[13] AS MOD_CRELATION, SPLIT(MOD,'~')[14] AS MOD_CREMARKS, SPLIT(MOD,'~')[15] AS MOD_CIDTYPE, SPLIT(MOD,'~')[16] AS MOD_CIDNO, SPLIT(MOD,'~')[17] AS MOD_CDEVICENO, SPLIT(MOD,'~')[18] AS MOD_CSIMNO, SPLIT(MOD,'~')[19] AS MOD_CGPSLAT, SPLIT(MOD,'~')[20] AS MOD_CGPSLON, SPLIT(MOD,'~')[21] AS MOD_DTRACK_INSERT, SPLIT(MOD,'~')[22] AS MOD_DTRACK_UPDATE, SPLIT(MOD,'~')[23] AS MOD_CSTATTIME, SPLIT(MOD,'~')[24] AS MOD_CAREA, SPLIT(MOD,'~')[25] AS MOD_CLOCCODE, SPLIT(MOD,'~')[26] AS MOD_DEDPUPDDT, SPLIT(MOD,'~')[27] AS MOD_DSYNCDATE, SPLIT(MOD,'~')[28] AS MOD_CSTATUS, SPLIT(MOD,'~')[29] AS MOD_CGPSTIME, SPLIT(MOD,'~')[30] AS MOD_CGPSSATCNT, SPLIT(MOD,'~')[31] AS MOD_OP_TS, SPLIT(MOD,'~')[32] AS MOD_OP_TYPE,SPLIT(MOD,'~')[33] AS MOD_LOADTS,SPLIT(SFM,'~')[0] AS SFM_CAWBNO, SPLIT(SFM,'~')[1] AS SFM_CMPSNO, SPLIT(SFM,'~')[2] AS SFM_NRUNID, SPLIT(SFM,'~')[3] AS SFM_DRUNDATE, SPLIT(SFM,'~')[4] AS SFM_CRUNCODE, SPLIT(SFM,'~')[5] AS SFM_CORGAREA, SPLIT(SFM,'~')[6] AS SFM_CSTATTYPE, SPLIT(SFM,'~')[7] AS SFM_CSTATCODE, SPLIT(SFM,'~')[8] AS SFM_CEMPLCODE, SPLIT(SFM,'~')[9] AS SFM_CLOCCODE, SPLIT(SFM,'~')[10] AS SFM_NPARRENTCONTID, SPLIT(SFM,'~')[11] AS SFM_DDTINSCAN, SPLIT(SFM,'~')[12] AS SFM_CTALLY, SPLIT(SFM,'~')[13] AS SFM_BPRINTED, SPLIT(SFM,'~')[14] AS SFM_DSTATDATE, SPLIT(SFM,'~')[15] AS SFM_ISUNDELSHP, SPLIT(SFM,'~')[16] AS SFM_ISUNDELIVERDATE, SPLIT(SFM,'~')[17] AS SFM_ISNOTOUTSCAN, SPLIT(SFM,'~')[18] AS SFM_ISNOTOUTSCANDATE, SPLIT(SFM,'~')[19] AS SFM_CRECDATA, SPLIT(SFM,'~')[20] AS SFM_DOUTSCANDT, SPLIT(SFM,'~')[21] AS SFM_BOUTSCAN, SPLIT(SFM,'~')[22] AS SFM_BEDPTRNX, SPLIT(SFM,'~')[23] AS SFM_CDSTAREA, SPLIT(SFM,'~')[24] AS SFM_CPTALLY, SPLIT(SFM,'~')[25] AS SFM_NRPCS, SPLIT(SFM,'~')[26] AS SFM_CDOCATTACH, SPLIT(SFM,'~')[27] AS SFM_NOPCS, SPLIT(SFM,'~')[28] AS SFM_CFLIGHTNO, SPLIT(SFM,'~')[29] AS SFM_DFLIGHTDT, SPLIT(SFM,'~')[30] AS SFM_DLOADARRDATE, SPLIT(SFM,'~')[31] AS SFM_DFLTARRDATE, SPLIT(SFM,'~')[32] AS SFM_CVEHICLENO, SPLIT(SFM,'~')[33] AS SFM_NENDKMS, SPLIT(SFM,'~')[34] AS SFM_DPTALLYDATE, SPLIT(SFM,'~')[35] AS SFM_CACTFLIGHTNO, SPLIT(SFM,'~')[36] AS SFM_DACTFLIGHTDT, SPLIT(SFM,'~')[37] AS SFM_CTALLYSRC, SPLIT(SFM,'~')[38] AS SFM_DDSTARRDT, SPLIT(SFM,'~')[39] AS SFM_CDLVPURTCD, SPLIT(SFM,'~')[40] AS SFM_CTALLYPURTCD, SPLIT(SFM,'~')[41] AS SFM_NACTPARRENTCONTID, SPLIT(SFM,'~')[42] AS SFM_CSTATEMPLCODE, SPLIT(SFM,'~')[43] AS SFM_CSTATCLRACTION, SPLIT(SFM,'~')[44] AS SFM_CSTATCLREMPLCODE, SPLIT(SFM,'~')[45] AS SFM_DSTATCLRDATE, SPLIT(SFM,'~')[46] AS SFM_CMSTATCODE, SPLIT(SFM,'~')[47] AS SFM_OP_TS, SPLIT(SFM,'~')[48] AS SFM_OP_TYPE,SPLIT(SFM,'~')[49] AS SFM_LOADTS, PDJ AS pod_journey,SPLIT(ALK,'~')[0] AS ALK_COAWBNO, SPLIT(ALK,'~')[1] AS ALK_COORGAREA, SPLIT(ALK,'~')[2] AS ALK_CODSTAREA, SPLIT(ALK,'~')[3] AS ALK_CNAWBNO, SPLIT(ALK,'~')[4] AS ALK_CNORGAREA, SPLIT(ALK,'~')[5] AS ALK_CNDSTAREA, SPLIT(ALK,'~')[6] AS ALK_NAMT, SPLIT(ALK,'~')[7] AS ALK_CPRODCODE, SPLIT(ALK,'~')[8] AS ALK_DDATE, SPLIT(ALK,'~')[9] AS ALK_DFLIGHTDT, SPLIT(ALK,'~')[10] AS ALK_CFLAG, SPLIT(ALK,'~')[11] AS ALK_CPRODTYPE, SPLIT(ALK,'~')[12] AS ALK_CFLIGHTNO, SPLIT(ALK,'~')[13] AS ALK_CLOCCODE, SPLIT(ALK,'~')[14] AS ALK_DDFILEDATE, SPLIT(ALK,'~')[15] AS ALK_OP_TS, SPLIT(ALK,'~')[16] AS ALK_OP_TYPE, SPLIT(ALK,'~')[17] AS ALK_LOADTS,SPLIT(SRT,'~')[0] AS SRT_CAWBNO, SPLIT(SRT,'~')[1] AS SRT_CTYPE, SPLIT(SRT,'~')[2] AS SRT_CEMPLCODE, SPLIT(SRT,'~')[3] AS SRT_CTIMEAREA, SPLIT(SRT,'~')[4] AS SRT_DATTEMPDT, SPLIT(SRT,'~')[5] AS SRT_CEMPLEMAIL, SPLIT(SRT,'~')[6] AS SRT_CEMPLNAME, SPLIT(SRT,'~')[7] AS SRT_CATTENTION, SPLIT(SRT,'~')[8] AS SRT_CNAWBNO, SPLIT(SRT,'~')[9] AS SRT_DENTDATE, SPLIT(SRT,'~')[10] AS SRT_CENTTIME, SPLIT(SRT,'~')[11] AS SRT_CORGAREA, SPLIT(SRT,'~')[12] AS SRT_CDSTAREA, SPLIT(SRT,'~')[13] AS SRT_CPRODCODE, SPLIT(SRT,'~')[14] AS SRT_CCUSTCODE, SPLIT(SRT,'~')[15] AS SRT_CMODE, SPLIT(SRT,'~')[16] AS SRT_CMAILLOC, SPLIT(SRT,'~')[17] AS SRT_CREMARKS, SPLIT(SRT,'~')[18] AS SRT_CCUSTNAME, SPLIT(SRT,'~')[19] AS SRT_CTYP, SPLIT(SRT,'~')[20] AS SRT_BSRYALTINST, SPLIT(SRT,'~')[21] AS SRT_CRECDBY, SPLIT(SRT,'~')[22] AS SRT_CRELATION, SPLIT(SRT,'~')[23] AS SRT_CSTATCODE, SPLIT(SRT,'~')[24] AS SRT_CPUWI, SPLIT(SRT,'~')[25] AS SRT_DIMPORTDT, SPLIT(SRT,'~')[26] AS SRT_OPTS, SPLIT(SRT,'~')[27] AS SRT_OPTYPE, SPLIT(SRT,'~')[28] AS SRT_LOADTS,SFJ as sfm_journey FROM hbasetab""")

        IncrLayer_DF.registerTempTable("incrlayer")

        println("After creating incrlayer")

        config.set(TableInputFormat.INPUT_TABLE, PL_tableName_AWBNO)

        val hBaseRDDPL = sc.newAPIHadoopRDD(config, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

        import sqlContext.implicits._

        /*Start appending Shipfltmst journey after reading IL layer*/

        val AWBNO_IL = IncrLayer_DF.select(col("PK").alias("ano"))

        println("Existing PL Table read for SFJ start:" + sdf.format(new Date()))

        val sfj_pl = hBaseRDDPL.map(x => {
          (
            Bytes.toString(x._2.getRow),
            Bytes.toString(x._2.getValue(Bytes.toBytes("SFJ"), Bytes.toBytes("CT_COL"))))
        }).toDF().withColumnRenamed("_1", "PK").withColumnRenamed("_2", "SFJ")

        println("After reading Existing PL data as a dataframe " + sdf.format(new Date()))

        val existing_SFJ_PL = AWBNO_IL.join(sfj_pl, sfj_pl("PK") === AWBNO_IL("ano"), "inner").select(sfj_pl("PK").alias("CAWBNO"), sfj_pl("SFJ").alias("CT_COL"))



     //********************Start creating POD journey after reading IL Layer**********************

     val podjourney_df = sqlContext.sql("""SELECT * FROM incrlayer where OBS_CAWBNO!='NA!@#' OR IBS_CAWBNO!='NA!@#'""")
        podjourney_df.registerTempTable("podjrny")
 
  //********************End creating POD journey after reading IL Layer**********************

        //val AWBNO = podjourney_df.select(expr("CONCAT('UD~',PK)").alias("ano"))
        val AWBNO = podjourney_df.select(col("PK").alias("ano"))

        println("Existing PL Table read start:" + sdf.format(new Date()))

        val df_pl = hBaseRDDPL.map(x => {
          (
            Bytes.toString(x._2.getRow),
            Bytes.toString(x._2.getValue(Bytes.toBytes("PDJ"), Bytes.toBytes("CT_COL"))))
        }).toDF().withColumnRenamed("_1", "PK").withColumnRenamed("_2", "PDJ")

        println("After reading Existing PL data as a dataframe " + sdf.format(new Date()))

        val existing_PL = AWBNO.join(df_pl, df_pl("PK") === AWBNO("ano"), "inner").select(df_pl("PK").alias("CAWBNO"), df_pl("PDJ").alias("CT_COL"))

        println("PL Table read completed::: " + sdf.format(new Date()))

/*        val ILWithPodJourney = sqlContext.sql("""select incrlayer.*,podjrny.pod_journey from incrlayer left outer join podjrny on podjrny.PK = incrlayer.PK""")
        ILWithPodJourney.registerTempTable("incrlayer")*/
        val closure_codes = prop.getProperty(CLOSURE_CODES)

        val delivery_stat = sqlContext.sql(s"""select PK as del_pk,CASE WHEN (IBS_CSTATTYPE = 'T' and IBS_CSTATCODE in ($closure_codes)) THEN CONCAT(IBS_CSTATTYPE,IBS_CSTATCODE)  WHEN (OBS_CSTATTYPE = 'T' and OBS_CSTATCODE in ($closure_codes) ) THEN  CONCAT(OBS_CSTATTYPE,OBS_CSTATCODE) ELSE 'UD' END as delivery_status FROM incrlayer where OBS_CAWBNO!='NA!@#' OR IBS_CAWBNO!='NA!@#'""")

        val delivery_date1 = delivery_stat.filter($"delivery_status" !== "UD").withColumn("ddate_load_ts",expr("from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm')"))

        delivery_date1.registerTempTable("ddate")

        var delivery_date:DataFrame = null;
        delivery_date = sqlContext.sql(""" select NVL(TRIM(del_pk),'NA!@#') as PK , ddate_load_ts as CT_COL from ddate """)

        println("Delivery status date completed")

        /* Start - Create POD Journey for Undelivered Records*/
        val PL_layer1 = IncrLayer_DF.join(delivery_stat,delivery_stat("del_pk")=== IncrLayer_DF("PK"),"left").drop($"del_pk")
        val PL_Layer = PL_layer1.withColumnRenamed("PK", "New_RowKey")
        
        PL_Layer.registerTempTable("pldf")

        val pod_IL = sqlContext.sql("""select New_RowKey as PK,pod_journey  from pldf where OBS_CAWBNO!='NA!@#' OR IBS_CAWBNO!='NA!@#'""")
        
        val joinPodJourney = pod_IL.join(existing_PL, existing_PL("CAWBNO") === pod_IL("PK"), "left").select(col("PK"), col("pod_journey").alias("IL_Journey"), col("CT_COL").alias("PL_Journey"))

        //joinPodJourney.printSchema
        joinPodJourney.registerTempTable("joinpod")

        val finalPodJourney = sqlContext.sql("""SELECT PK,CASE WHEN PL_Journey != 'NA!@#' OR PL_Journey is not null THEN CONCAT_WS('|',PL_Journey,IL_Journey) ELSE IL_Journey END AS journey from joinpod""")
        finalPodJourney.registerTempTable("finalpod")
        /*SFJ journey append*/

        val sfj_IL = sqlContext.sql("""select New_RowKey as PK,sfm_journey  from pldf where SFM_CAWBNO!='NA!@#'""")

        val joinSfjJourney = sfj_IL.join(existing_SFJ_PL, existing_SFJ_PL("CAWBNO") === sfj_IL("PK"), "left").select(col("PK"), col("sfm_journey").alias("IL_Journey"), col("CT_COL").alias("PL_Journey"))

        //joinPodJourney.printSchema
        joinSfjJourney.registerTempTable("joinsfj")

        val finalSFMJourney = sqlContext.sql("""SELECT PK,CASE WHEN PL_Journey != 'NA!@#' OR PL_Journey is not null THEN CONCAT_WS('|',PL_Journey,IL_Journey) ELSE IL_Journey END AS journey from joinsfj""")
        finalSFMJourney.registerTempTable("finalsfj")

        /* End - Create POD Journey for Undelivered Records*/
        //***PACKMST and CUSTMST Join with AWBMST and SHIPCLT*************
        try{
          custmstdf = sqlContext.read.parquet(MSTDATA_PATH+"Ops_CT_Mst_Custmst")
          packmstDF = sqlContext.read.parquet(MSTDATA_PATH+"Ops_CT_Mst_Packmst")
          cmgrpmstDF = sqlContext.read.parquet(MSTDATA_PATH+"Ops_CT_Mst_Cmpgmst")
          println("<<<<<READING MASTER Primary>>>>>>")

        }catch{
          case ex: Throwable => {
            custmstdf = sqlContext.read.parquet(MSTBCK_PATH+"Ops_CT_Mst_Custmst")
            packmstDF = sqlContext.read.parquet(MSTBCK_PATH+"Ops_CT_Mst_Packmst")
            cmgrpmstDF = sqlContext.read.parquet(MSTBCK_PATH+"Ops_CT_Mst_Cmpgmst")

            println("<<<<<READING MASTER Backup>>>>>>")
          }
        }
        /*custmstdf.write.mode("overwrite").parquet(TEMPPATH+"custmst.parquet")
        custmstdf = sqlContext.read.parquet(TEMPPATH+"custmst.parquet")

        packmstDF.write.mode("overwrite").parquet(TEMPPATH+"packmst.parquet")
        packmstDF = sqlContext.read.parquet(TEMPPATH+"packmst.parquet")

        cmgrpmstDF.write.mode("overwrite").parquet(TEMPPATH+"cmgrpmst.parquet")
        cmgrpmstDF = sqlContext.read.parquet(TEMPPATH+"cmgrpmst.parquet")*/

        custmstdf.registerTempTable("custmstdf")
        packmstDF.registerTempTable("packmstDF")
        cmgrpmstDF.registerTempTable("cmgrpmstDF")

        val awbmstDF1 = sqlContext.sql("""select New_RowKey as PK,ABS_CAWBNO, ABS_NAWBID, ABS_CPRODCODE, ABS_CPRODTYPE, ABS_CORGAREA, ABS_CDSTAREA, ABS_CBATCHNO, ABS_NPCS, ABS_DSHIPDATE, ABS_DPUDATE, ABS_NTOKENNO, ABS_CPUWI, ABS_NDIMWEIGHT, ABS_CCUSTCODE, ABS_NCSHMEMNO, ABS_CCMDTYCODE, ABS_NCOLAMT, ABS_NAMT, ABS_CTRNCODE, ABS_CSPLINST, ABS_CPACKTYPE, ABS_CODA, ABS_CMODE, ABS_CGSACODE, ABS_CFODCODFLG, ABS_CFOCCODE, ABS_CBTPCODE, ABS_CBTPAREA, ABS_CBILLNO, ABS_BREFNO, ABS_BDUTY, ABS_CSECTYCK, ABS_NPARRENTCONTID, ABS_BBILLCNEE, ABS_CPUEMPCODE, ABS_BSTAMDART, ABS_CORGSCRCD, ABS_CDSTSCRCD, ABS_BPRIORITY, ABS_CSUBCODE, ABS_NCURLOOKUPID, ABS_CPUTIME, ABS_CFLFM, ABS_CDHLACCTNO, ABS_CRTOAREA, ABS_CRTOCODE, ABS_CEMAILID, ABS_NWEIGHT, ABS_NOCTROI, ABS_NDUTY, ABS_NSMARTBOXTYPEA, ABS_NSMARTBOXTYPEB, ABS_NDCCOUNT, ABS_CBILLCAC, ABS_BCRCRDREF, ABS_NDCRECD, ABS_CDLVPURTCD, ABS_CACCURACY, ABS_CDLCODE, ABS_CKGPOUND, ABS_CMLOCCODE, ABS_CADECODE, ABS_DINSCANDT, ABS_CINSCANLOC, ABS_BEDPTRNX, ABS_BCRCRDPAY, ABS_BIATADTL, ABS_CDOCATTACH, ABS_CEMPLCODE, ABS_NOPCS, ABS_CMUSTGOCRG, ABS_CODOCATTAC, ABS_CDATAENTRYLOC, ABS_BSHPCRCRDREF, ABS_DEPTDTDLV, ABS_BDETAIN, ABS_CISOVERAGE, ABS_CDHLFLAG, ABS_DDEMUDT, ABS_NDEMUAMT, ABS_CDEMUCODE, ABS_CDEMULOCCODE, ABS_DSTATUSDT, ABS_BCSBPRINTED, ABS_DDATAENTRYDT, ABS_DBATCHDT, ABS_CASTATTYPE, ABS_CASTATCODE, ABS_CSTATEMPLCODE, ABS_CAREMARKS, ABS_CAPTCODE, ABS_BPWPALLETIZED, ABS_CPRINTMODE, ABS_CPROMOCODE, ABS_CRTOIMTLY, ABS_BDGSHIPMENT, ABS_ISPWREC, ABS_CPREFTM, ABS_CREVPU, ABS_CPSCODE, ABS_CFRDAWBNO, ABS_CRFDCOMPNM, ABS_CREFNO2, ABS_CREFNO3, ABS_CPUMODE, ABS_CPUTYPE, ABS_NITEMCNT, ABS_BPARTIALPU, ABS_NPAYCASH, ABS_NPUTMSLOT, ABS_CMANIFSTNO, ABS_COFFCLTIME, ABS_NDEFERREDDELIVERYDAYS, ABS_DCUSTEDD, ABS_CISDDN, ABS_CACTDELLOC, ABS_CGSTNO, ABS_CVEHICLENO, ABS_CEXCHAWB, ABS_DPREFDATE, ABS_CPREFTIME, ABS_NDISTANCE, ABS_DCUSTPUDT, ABS_CCUSTPUTM, ABS_CAVAILTIME, ABS_CAVAILDAYS, ABS_COTPTYPE, ABS_NOTPNO, ABS_CPACKAGINGID, ABS_CCCODE, ABS_OP_TS, ABS_OP_TYPE,ABS_LOADTS from pldf""")
        
        awbmstDF1.registerTempTable("awbmst_parquet_TBL")

        val packmst_custmst_join_awbmstDF_record = sqlContext.sql("""select awb.*,cust.ccompgrp as CUST_ccompgrp, pack.CPRODPARENT as PACK_cprodparent, pack.CPRODCOMMNAME as PACK_cprodcommname, pack.CSUBPRDCOMMNAME as PACK_csubprdcommname,pack.CPRODGROUP as PACK_cprodgroup from awbmst_parquet_TBL awb left outer join custmstdf cust on if(awb.ABS_CBTPAREA= 'NA!@#',awb.ABS_CORGAREA,ABS_CBTPAREA) = nvl(cust.CAREA,'NA!@#') and if(awb.ABS_CBTPCODE= 'NA!@#',awb.ABS_CCUSTCODE,ABS_CBTPCODE) = nvl(cust.ccustcode,'NA!@#') left outer join packmstDF pack on awb.ABS_CPRODCODE = nvl(pack.cprodcode,'NA!@#') and awb.ABS_CMODE = nvl(pack.csubprdcd,'NA!@#') and awb.ABS_CPACKTYPE = nvl(pack.cpacktype,'NA!@#')""")

        println("packmst_custmst_join_awbmstDF_record join")

packmst_custmst_join_awbmstDF_record.registerTempTable("awbpack")

        val packmst_custmst_cmpgrpname_join_awbmstDF_record = sqlContext.sql("""select n.*,cmp.ccompname as cgrpname from awbpack n left join cmgrpmstDF cmp on cmp.ccompgrp=n.CUST_ccompgrp """)

        println("packmst_custmst_cmpgrpname_join_awbmstDF_record show")

        packmst_custmst_cmpgrpname_join_awbmstDF_record.registerTempTable("awbpack")

        val shipcltDF1 = sqlContext.sql("""select New_RowKey as PK,SLT_CAWBNO, SLT_CBATCHNO, SLT_CBATORG, SLT_CPRODCODE, SLT_CORGAREA, SLT_CORGSCRCD, SLT_CMAWBNO, SLT_CMLOCCODE, SLT_CDSTAREA, SLT_CDSTSCRCD, SLT_CMODE, SLT_CPRODTYPE, SLT_CTRNCODE, SLT_CFOCCODE, SLT_BPRIORITY, SLT_CCUSTCODE, SLT_CSENDER, SLT_CCNEECODE, SLT_CATTENTION, SLT_NPCS, SLT_NWEIGHT, SLT_NACTWGT, SLT_CCMDTYCODE, SLT_NTOKENNO, SLT_NCSHMEMNO, SLT_NAMT, SLT_CBILLNO, SLT_BBILLCNEE, SLT_CBILLCAC, SLT_CPACKTYPE, SLT_DSHIPDATE, SLT_DPUDATE, SLT_CPUTIME, SLT_CPUEMPLCD, SLT_CPUWI, SLT_DEPTDTDLV, SLT_CODA, SLT_CFLIGHTNO, SLT_DFLIGHTDT, SLT_CKGPOUND, SLT_BLINKED, SLT_CADECODE, SLT_CCRCRDREF, SLT_NDIML, SLT_NDIMB, SLT_NDIMH, SLT_NSLABWGT, SLT_NASSDVALUE, SLT_CDOCNO, SLT_DDOCDATE, SLT_CPAYTYPE, SLT_NAMOUNT, SLT_CINVNO, SLT_DINVDATE, SLT_NOTHCHRGS, SLT_NCDUTYPC, SLT_CCRCARDNO, SLT_CCARDCODE, SLT_CCARDHOLD, SLT_DVALIDUPTO, SLT_CCUSTNAME, SLT_CCUSTADR1, SLT_CCUSTADR2, SLT_CCUSTADR3, SLT_CCUSTPIN, SLT_CCUSTTEL, SLT_CCUSTFAX, SLT_CCNEENAME, SLT_CCNEEADR1, SLT_CCNEEADR2, SLT_CCNEEADR3, SLT_CCNEEPIN, SLT_CCNEETEL, SLT_CCNEEFAX, SLT_BCHECKLST, SLT_CSPLINST, SLT_CPRODDESC, SLT_DBATCHDT, SLT_NOCTROI, SLT_CCLECTYPE, SLT_NDCLRDVAL, SLT_BSTAMPDART, SLT_CCMDTYDESC, SLT_CCALTNAME, SLT_CALTATTN, SLT_CCALTADR1, SLT_CCALTADR2, SLT_CCALTADR3, SLT_CCALTPIN, SLT_CCALTTEL, SLT_CCALTFAX, SLT_CCNEEMOB, SLT_CCALTMOB, SLT_NCOLAMT, SLT_CFODCODFLG, SLT_CSUBCODE, SLT_CCTMNO, SLT_BDOXATCHD, SLT_CMRKSNOS1, SLT_CMRKSNOS2, SLT_CMRKSNOS3, SLT_CDIMEN1, SLT_CDIMEN2, SLT_CDIMEN3, SLT_NCHRGWT, SLT_NCOMVAL, SLT_NFREIGHT, SLT_NVALCHGS, SLT_NAWBFEE, SLT_CAWBFEE, SLT_NSTATCHG, SLT_CSTATCHG, SLT_NCARTCHG, SLT_CCARTCHG, SLT_NREGTCHG, SLT_CREGTCHG, SLT_NMISCCHG1, SLT_CMISCCHG1, SLT_NMISCCHG2, SLT_CMISCCHG2, SLT_NCHGCOLCT, SLT_COCTRCPTNO, SLT_CEMAILID, SLT_CBTPAREA, SLT_CBTPCODE, SLT_CCODFAVOUR, SLT_CCODPAYBLE, SLT_CFOVTYPE, SLT_NADDISRCHG, SLT_NADDOSRCHG, SLT_NDOCCHRG, SLT_NDCCHRG, SLT_NFODCHRG, SLT_NRISKHCHGS, SLT_NODACHRG, SLT_CGSACODE, SLT_NFSAMT, SLT_CDHLFLAG, SLT_NDODAMT, SLT_CLOCCODE, SLT_CILLOC, SLT_CDLCODE, SLT_NDIWT, SLT_CCUSTMOB, SLT_CADNLCRCRDREF, SLT_DUPLDATE, SLT_CREGCUSTNAME, SLT_CREGCUSTADR1, SLT_CREGCUSTADR2, SLT_CREGCUSTADR3, SLT_CREGCUSTPIN, SLT_CREGCUSTTEL, SLT_CRTOIMDTLY, SLT_CPREFTM, SLT_CREVPU, SLT_CPSCODE, SLT_DDWNLDDATE, SLT_CFRDAWBNO, SLT_CRFDCOMPNM, SLT_CREFNO2, SLT_CREFNO3, SLT_CPUMODE, SLT_CPUTYPE, SLT_NITEMCNT, SLT_BPARTIALPU, SLT_NPAYCASH, SLT_NPUTMSLOT, SLT_CCNEEMAIL, SLT_CRTOCONTNM, SLT_CRTOADR1, SLT_CRTOADR2, SLT_CRTOADR3, SLT_CRTOPIN, SLT_CRTOTEL, SLT_CRTOMOB, SLT_CMANIFSTNO, SLT_CRTOLAT, SLT_CRTOLON, SLT_CRTOADRDT, SLT_CCUSTLAT, SLT_CCUSTLON, SLT_CCUSTADRDT, SLT_CCNEELAT, SLT_CCNEELON, SLT_CCNEEADRDT, SLT_CCALTLAT, SLT_CCALTLON, SLT_CCALTADRDT, SLT_CREGCUSTLAT, SLT_CREGCUSTLON, SLT_CREGCUSTADRDT, SLT_COFFCLTIME, SLT_NDEFERREDDELIVERYDAYS, SLT_CFLFM, SLT_CCNTRYCODE, SLT_CSTATECODE, SLT_DCUSTEDD, SLT_CISDDN, SLT_CACTDELLOC, SLT_CGSTNO, SLT_CCUSTGSTNO, SLT_CCNEEGSTNO, SLT_CEXCHAWB, SLT_CCNEEFADD, SLT_DCUSTPUDT, SLT_CCUSTPUTM, SLT_CCONMOBMASK, SLT_CADDRESSTYPE, SLT_CAVAILTIME, SLT_CAVAILDAYS, SLT_CCSTMOBMAS, SLT_CRTOMOBMAS, SLT_COTPTYPE, SLT_NOTPNO, SLT_CINCOTERMS, SLT_CPURTCD, SLT_CPULOC, SLT_CCOMPGRP, SLT_CPACKAGINGID, SLT_OP_TS, SLT_OP_TYPE,SLT_LOADTS from pldf""")

        shipcltDF1.registerTempTable("shipclt_record")

        val packmst_custmst_join_shipclt_record = sqlContext.sql("""select shp.*,cust.ccompgrp as CUST_ccompgrp, pack.CPRODPARENT as PACK_cprodparent, pack.CPRODCOMMNAME as PACK_cprodcommname, pack.CSUBPRDCOMMNAME as PACK_csubprdcommname,pack.CPRODGROUP as PACK_cprodgroup from shipclt_record shp left outer join custmstdf cust on if(shp.SLT_CBTPAREA = 'NA!@#',shp.SLT_CORGAREA,SLT_CBTPAREA) = nvl(cust.CAREA,'NA!@#') and if(shp.SLT_CBTPCODE= 'NA!@#',shp.SLT_CCUSTCODE,SLT_CBTPCODE) = nvl(cust.ccustcode,'NA!@#') left outer join packmstDF pack on shp.SLT_CPRODCODE = nvl(pack.cprodcode,'NA!@#') and shp.SLT_CMODE = nvl(pack.csubprdcd,'NA!@#') and shp.SLT_CPACKTYPE = nvl(pack.cpacktype,'NA!@#')""")
        
        packmst_custmst_join_shipclt_record.registerTempTable("sltpack")
        println("shipclt with master")

        val packmst_custmst_compgrp_join_shipclt_record = sqlContext.sql("""select n.*,cmp.ccompname as cgrpname from sltpack n left join cmgrpmstDF cmp on cmp.ccompgrp=n.CUST_ccompgrp """)

        println("packmst_custmst_compgrp_join_shipclt_record show")

        packmst_custmst_compgrp_join_shipclt_record.registerTempTable("sltpack")

        /**START - STATMST Table Read **/
        try{
          statmst_GG_DF = sqlContext.read.parquet(MSTDATA_PATH+"Ops_CT_Mst_Statmst").select(col("cstatcode").alias("stat_cstatcode"),col("cstattype").alias("stat_cstattype"),col("cstatgroup").alias("stat_cstatgroup"),col("cstatdesc").alias("stat_cstatdesc"))

        }catch{
          case ex: Throwable => {
            statmst_GG_DF = sqlContext.read.parquet(MSTBCK_PATH+"Ops_CT_Mst_Statmst").select(col("cstatcode").alias("stat_cstatcode"),col("cstattype").alias("stat_cstattype"),col("cstatgroup").alias("stat_cstatgroup"),col("cstatdesc").alias("stat_cstatdesc"))
          }
        }
        /**END - STATMST Table Read **/


        /***************************DUMP Data to HBASE PL Layer******************************/
        var awbmstDF :DataFrame = null;
        var inboundDF :DataFrame = null;
        var outboundDF:DataFrame = null;
        var shipcltDF:DataFrame = null;
        var podJourney:DataFrame = null;
        var mdpickup_DF:DataFrame = null;
        var mdpod_DF:DataFrame = null;
        var dls_DF:DataFrame = null;
        var ldts_DF:DataFrame = null;
        var sfm_DF:DataFrame = null;
        var sfmJourney:DataFrame = null;

        /*val outboundDF = sqlContext.sql("""select NVL(TRIM(New_RowKey),'NA!@#') as PK,CONCAT_WS('~',NVL(OBS_CAWBNO,'NA!@#'), NVL(OBS_NSTATUSID,'NA!@#'), NVL(OBS_NOPERATIONID,'NA!@#'), NVL(OBS_CPRODCODE,'NA!@#'), NVL(OBS_CORGAREA,'NA!@#'), NVL(OBS_CDSTAREA,'NA!@#'), NVL(OBS_CMPSNO,'NA!@#'), NVL(OBS_CSTATCODE,'NA!@#'), NVL(OBS_CSTATTYPE,'NA!@#'), NVL(OBS_DSTATDATE,'NA!@#'), NVL(OBS_CREMARKS,'NA!@#'), NVL(OBS_CLOCCODE,'NA!@#'), NVL(OBS_CEMPLCODE,'NA!@#'), NVL(OBS_CFILENAME,'NA!@#'), NVL(OBS_SEDPTRNX,'NA!@#'), NVL(OBS_DEDPTRNX,'NA!@#'), NVL(OBS_CCNO,'NA!@#'), NVL(OBS_DRUNDATE,'NA!@#'), NVL(OBS_DLASTMODIFIEDTS,'NA!@#'), NVL(OBS_OP_TS,'NA!@#'), NVL(OBS_OP_TYPE,'NA!@#')) as CT_COL from pldf where OBS_CAWBNO!='NA!@#' or OBS_CAWBNO is not null""")*/

        /** Start changes -- outbound join with statmst ***/

        val outboundDF1 = sqlContext.sql("""select New_RowKey as PK,OBS_CAWBNO, OBS_NSTATUSID, OBS_NOPERATIONID, OBS_CPRODCODE, OBS_CORGAREA, OBS_CDSTAREA, OBS_CMPSNO, OBS_CSTATCODE, OBS_CSTATTYPE, OBS_DSTATDATE, OBS_CREMARKS, OBS_CLOCCODE, OBS_CEMPLCODE, OBS_CFILENAME, OBS_SEDPTRNX, OBS_DEDPTRNX, OBS_CCNO, OBS_DRUNDATE, OBS_DLASTMODIFIEDTS, OBS_OP_TS, OBS_OP_TYPE ,OBS_LOADTS from pldf where OBS_CAWBNO!='NA!@#' or OBS_CAWBNO is not null""")

        val outbound_stat_DF = outboundDF1.join(statmst_GG_DF,outboundDF1("OBS_CSTATTYPE") === statmst_GG_DF("stat_cstattype") && outboundDF1("OBS_CSTATCODE") === statmst_GG_DF("stat_cstatcode"),"left")

        outbound_stat_DF.registerTempTable("obstatjoin")

         outboundDF = sqlContext.sql("""select NVL(TRIM(PK),'NA!@#') as PK,CONCAT_WS('~',NVL(OBS_CAWBNO,'NA!@#'), NVL(OBS_NSTATUSID,'NA!@#'), NVL(OBS_NOPERATIONID,'NA!@#'), NVL(OBS_CPRODCODE,'NA!@#'), NVL(OBS_CORGAREA,'NA!@#'), NVL(OBS_CDSTAREA,'NA!@#'), NVL(OBS_CMPSNO,'NA!@#'), NVL(OBS_CSTATCODE,'NA!@#'), NVL(OBS_CSTATTYPE,'NA!@#'), NVL(OBS_DSTATDATE,'NA!@#'), NVL(OBS_CREMARKS,'NA!@#'), NVL(OBS_CLOCCODE,'NA!@#'), NVL(OBS_CEMPLCODE,'NA!@#'), NVL(OBS_CFILENAME,'NA!@#'), NVL(OBS_SEDPTRNX,'NA!@#'), NVL(OBS_DEDPTRNX,'NA!@#'), NVL(OBS_CCNO,'NA!@#'), NVL(OBS_DRUNDATE,'NA!@#'), NVL(OBS_DLASTMODIFIEDTS,'NA!@#'), NVL(OBS_OP_TS,'NA!@#'), NVL(OBS_OP_TYPE,'NA!@#'), NVL(stat_cstatgroup,'NA!@#'), NVL(stat_cstatdesc,'NA!@#')) as CT_COL from obstatjoin where OBS_CAWBNO!='NA!@#' or OBS_CAWBNO is not null""")

        println("outbound show")

        /** End changes -- outbound join with statmst ***/

        /*val inboundDF = sqlContext.sql("""select NVL(TRIM(New_RowKey),'NA!@#') as PK,CONCAT_WS('~',NVL(IBS_CAWBNO,'NA!@#'), NVL(IBS_NSTATUSID,'NA!@#'), NVL(IBS_NOPERATIONID,'NA!@#'), NVL(IBS_CMPSNO,'NA!@#'), NVL(IBS_CSTATCODE,'NA!@#'), NVL(IBS_CSTATTYPE,'NA!@#'), NVL(IBS_DSTATDATE,'NA!@#'), NVL(IBS_CREMARKS,'NA!@#'), NVL(IBS_CLOCCODE,'NA!@#'), NVL(IBS_CEMPLCODE,'NA!@#'), NVL(IBS_CFILENAME,'NA!@#'), NVL(IBS_SEDPTRNX,'NA!@#'), NVL(IBS_DEDPTRNX,'NA!@#'), NVL(IBS_DFILENAME,'NA!@#'), NVL(IBS_CPRODCODE,'NA!@#'), NVL(IBS_CORGAREA,'NA!@#'), NVL(IBS_CDSTAREA,'NA!@#'), NVL(IBS_DENTDATE,'NA!@#'), NVL(IBS_DLASTMODIFIEDTS,'NA!@#'), NVL(IBS_OP_TS,'NA!@#'), NVL(IBS_OP_TYPE,'NA!@#')) as CT_COL from pldf where IBS_CAWBNO!='NA!@#' or IBS_CAWBNO is not null""")*/

        /*** start changes - inbound join with statmst ***/

        val inboundDF1 = sqlContext.sql("""select New_RowKey as PK,IBS_CAWBNO, IBS_NSTATUSID, IBS_NOPERATIONID, IBS_CMPSNO, IBS_CSTATCODE, IBS_CSTATTYPE, IBS_DSTATDATE, IBS_CREMARKS, IBS_CLOCCODE, IBS_CEMPLCODE, IBS_CFILENAME, IBS_SEDPTRNX, IBS_DEDPTRNX, IBS_DFILENAME, IBS_CPRODCODE, IBS_CORGAREA, IBS_CDSTAREA, IBS_DENTDATE, IBS_DLASTMODIFIEDTS, IBS_OP_TS, IBS_OP_TYPE from pldf where IBS_CAWBNO!='NA!@#' or IBS_CAWBNO is not null""")

        val inbound_stat_join = inboundDF1.join(statmst_GG_DF,inboundDF1("IBS_CSTATTYPE") === statmst_GG_DF("stat_cstattype") && inboundDF1("IBS_CSTATCODE") === statmst_GG_DF("stat_cstatcode"),"left")

        inbound_stat_join.registerTempTable("ibstatjoin")

         inboundDF = sqlContext.sql("""select NVL(TRIM(PK),'NA!@#') as PK,CONCAT_WS('~',NVL(IBS_CAWBNO,'NA!@#'), NVL(IBS_NSTATUSID,'NA!@#'), NVL(IBS_NOPERATIONID,'NA!@#'), NVL(IBS_CMPSNO,'NA!@#'), NVL(IBS_CSTATCODE,'NA!@#'), NVL(IBS_CSTATTYPE,'NA!@#'), NVL(IBS_DSTATDATE,'NA!@#'), NVL(IBS_CREMARKS,'NA!@#'), NVL(IBS_CLOCCODE,'NA!@#'), NVL(IBS_CEMPLCODE,'NA!@#'), NVL(IBS_CFILENAME,'NA!@#'), NVL(IBS_SEDPTRNX,'NA!@#'), NVL(IBS_DEDPTRNX,'NA!@#'), NVL(IBS_DFILENAME,'NA!@#'), NVL(IBS_CPRODCODE,'NA!@#'), NVL(IBS_CORGAREA,'NA!@#'), NVL(IBS_CDSTAREA,'NA!@#'), NVL(IBS_DENTDATE,'NA!@#'), NVL(IBS_DLASTMODIFIEDTS,'NA!@#'), NVL(IBS_OP_TS,'NA!@#'), NVL(IBS_OP_TYPE,'NA!@#'), NVL(stat_cstatgroup,'NA!@#'), NVL(stat_cstatdesc,'NA!@#')) as CT_COL from ibstatjoin where IBS_CAWBNO!='NA!@#' or IBS_CAWBNO is not null""")

        println("inboundDF show")

        /** end changes - inbound join with statmst **/

         awbmstDF = sqlContext.sql("""select NVL(TRIM(PK),'NA!@#') as PK,CONCAT_WS('~',NVL(ABS_CAWBNO,'NA!@#'), NVL(ABS_NAWBID,'NA!@#'), NVL(ABS_CPRODCODE,'NA!@#'), NVL(ABS_CPRODTYPE,'NA!@#'), NVL(ABS_CORGAREA,'NA!@#'), NVL(ABS_CDSTAREA,'NA!@#'), NVL(ABS_CBATCHNO,'NA!@#'), NVL(ABS_NPCS,'NA!@#'), NVL(ABS_DSHIPDATE,'NA!@#'), NVL(ABS_DPUDATE,'NA!@#'), NVL(ABS_NTOKENNO,'NA!@#'), NVL(ABS_CPUWI,'NA!@#'), NVL(ABS_NDIMWEIGHT,'NA!@#'), NVL(ABS_CCUSTCODE,'NA!@#'), NVL(ABS_NCSHMEMNO,'NA!@#'), NVL(ABS_CCMDTYCODE,'NA!@#'), NVL(ABS_NCOLAMT,'NA!@#'), NVL(ABS_NAMT,'NA!@#'), NVL(ABS_CTRNCODE,'NA!@#'), NVL(ABS_CSPLINST,'NA!@#'), NVL(ABS_CPACKTYPE,'NA!@#'), NVL(ABS_CODA,'NA!@#'), NVL(ABS_CMODE,'NA!@#'), NVL(ABS_CGSACODE,'NA!@#'), NVL(ABS_CFODCODFLG,'NA!@#'), NVL(ABS_CFOCCODE,'NA!@#'), NVL(ABS_CBTPCODE,'NA!@#'), NVL(ABS_CBTPAREA,'NA!@#'), NVL(ABS_CBILLNO,'NA!@#'), NVL(ABS_BREFNO,'NA!@#'), NVL(ABS_BDUTY,'NA!@#'), NVL(ABS_CSECTYCK,'NA!@#'), NVL(ABS_NPARRENTCONTID,'NA!@#'), NVL(ABS_BBILLCNEE,'NA!@#'), NVL(ABS_CPUEMPCODE,'NA!@#'), NVL(ABS_BSTAMDART,'NA!@#'), NVL(ABS_CORGSCRCD,'NA!@#'), NVL(ABS_CDSTSCRCD,'NA!@#'), NVL(ABS_BPRIORITY,'NA!@#'), NVL(ABS_CSUBCODE,'NA!@#'), NVL(ABS_NCURLOOKUPID,'NA!@#'), NVL(ABS_CPUTIME,'NA!@#'), NVL(ABS_CFLFM,'NA!@#'), NVL(ABS_CDHLACCTNO,'NA!@#'), NVL(ABS_CRTOAREA,'NA!@#'), NVL(ABS_CRTOCODE,'NA!@#'), NVL(ABS_CEMAILID,'NA!@#'), NVL(ABS_NWEIGHT,'NA!@#'), NVL(ABS_NOCTROI,'NA!@#'), NVL(ABS_NDUTY,'NA!@#'), NVL(ABS_NSMARTBOXTYPEA,'NA!@#'), NVL(ABS_NSMARTBOXTYPEB,'NA!@#'), NVL(ABS_NDCCOUNT,'NA!@#'), NVL(ABS_CBILLCAC,'NA!@#'), NVL(ABS_BCRCRDREF,'NA!@#'), NVL(ABS_NDCRECD,'NA!@#'), NVL(ABS_CDLVPURTCD,'NA!@#'), NVL(ABS_CACCURACY,'NA!@#'), NVL(ABS_CDLCODE,'NA!@#'), NVL(ABS_CKGPOUND,'NA!@#'), NVL(ABS_CMLOCCODE,'NA!@#'), NVL(ABS_CADECODE,'NA!@#'), NVL(ABS_DINSCANDT,'NA!@#'), NVL(ABS_CINSCANLOC,'NA!@#'), NVL(ABS_BEDPTRNX,'NA!@#'), NVL(ABS_BCRCRDPAY,'NA!@#'), NVL(ABS_BIATADTL,'NA!@#'), NVL(ABS_CDOCATTACH,'NA!@#'), NVL(ABS_CEMPLCODE,'NA!@#'), NVL(ABS_NOPCS,'NA!@#'), NVL(ABS_CMUSTGOCRG,'NA!@#'), NVL(ABS_CODOCATTAC,'NA!@#'), NVL(ABS_CDATAENTRYLOC,'NA!@#'), NVL(ABS_BSHPCRCRDREF,'NA!@#'), NVL(ABS_DEPTDTDLV,'NA!@#'), NVL(ABS_BDETAIN,'NA!@#'), NVL(ABS_CISOVERAGE,'NA!@#'), NVL(ABS_CDHLFLAG,'NA!@#'), NVL(ABS_DDEMUDT,'NA!@#'), NVL(ABS_NDEMUAMT,'NA!@#'), NVL(ABS_CDEMUCODE,'NA!@#'), NVL(ABS_CDEMULOCCODE,'NA!@#'), NVL(ABS_DSTATUSDT,'NA!@#'), NVL(ABS_BCSBPRINTED,'NA!@#'), NVL(ABS_DDATAENTRYDT,'NA!@#'), NVL(ABS_DBATCHDT,'NA!@#'), NVL(ABS_CASTATTYPE,'NA!@#'), NVL(ABS_CASTATCODE,'NA!@#'), NVL(ABS_CSTATEMPLCODE,'NA!@#'), NVL(ABS_CAREMARKS,'NA!@#'), NVL(ABS_CAPTCODE,'NA!@#'), NVL(ABS_BPWPALLETIZED,'NA!@#'), NVL(ABS_CPRINTMODE,'NA!@#'), NVL(ABS_CPROMOCODE,'NA!@#'), NVL(ABS_CRTOIMTLY,'NA!@#'), NVL(ABS_BDGSHIPMENT,'NA!@#'), NVL(ABS_ISPWREC,'NA!@#'), NVL(ABS_CPREFTM,'NA!@#'), NVL(ABS_CREVPU,'NA!@#'), NVL(ABS_CPSCODE,'NA!@#'), NVL(ABS_CFRDAWBNO,'NA!@#'), NVL(ABS_CRFDCOMPNM,'NA!@#'), NVL(ABS_CREFNO2,'NA!@#'), NVL(ABS_CREFNO3,'NA!@#'), NVL(ABS_CPUMODE,'NA!@#'), NVL(ABS_CPUTYPE,'NA!@#'), NVL(ABS_NITEMCNT,'NA!@#'), NVL(ABS_BPARTIALPU,'NA!@#'), NVL(ABS_NPAYCASH,'NA!@#'), NVL(ABS_NPUTMSLOT,'NA!@#'), NVL(ABS_CMANIFSTNO,'NA!@#'), NVL(ABS_COFFCLTIME,'NA!@#'), NVL(ABS_NDEFERREDDELIVERYDAYS,'NA!@#'), NVL(ABS_DCUSTEDD,'NA!@#'), NVL(ABS_CISDDN,'NA!@#'), NVL(ABS_CACTDELLOC,'NA!@#'), NVL(ABS_CGSTNO,'NA!@#'), NVL(ABS_CVEHICLENO,'NA!@#'), NVL(ABS_CEXCHAWB,'NA!@#'), NVL(ABS_DPREFDATE,'NA!@#'), NVL(ABS_CPREFTIME,'NA!@#'), NVL(ABS_NDISTANCE,'NA!@#'), NVL(ABS_DCUSTPUDT,'NA!@#'), NVL(ABS_CCUSTPUTM,'NA!@#'), NVL(ABS_CAVAILTIME,'NA!@#'), NVL(ABS_CAVAILDAYS,'NA!@#'), NVL(ABS_COTPTYPE,'NA!@#'), NVL(ABS_NOTPNO,'NA!@#'), NVL(ABS_CPACKAGINGID,'NA!@#'), NVL(ABS_CCCODE,'NA!@#'), NVL(ABS_OP_TS,'NA!@#'), NVL(ABS_OP_TYPE,'NA!@#'),NVL(CUST_ccompgrp,'NA!@#'), NVL(PACK_cprodparent,'NA!@#'), NVL(PACK_cprodcommname,'NA!@#'), NVL(PACK_csubprdcommname,'NA!@#'), NVL(PACK_cprodgroup,'NA!@#'), NVL(cgrpname,'NA!@#') ) as CT_COL from awbpack where ABS_CAWBNO!='NA!@#' or ABS_CAWBNO is not null""")

        val shipcltDF_nondelete = sqlContext.sql("""select NVL(TRIM(PK),'NA!@#') as PK,CONCAT_WS('~',NVL(SLT_CAWBNO,'NA!@#'), NVL(SLT_CBATCHNO,'NA!@#'), NVL(SLT_CBATORG,'NA!@#'), NVL(SLT_CPRODCODE,'NA!@#'), NVL(SLT_CORGAREA,'NA!@#'), NVL(SLT_CORGSCRCD,'NA!@#'), NVL(SLT_CMAWBNO,'NA!@#'), NVL(SLT_CMLOCCODE,'NA!@#'), NVL(SLT_CDSTAREA,'NA!@#'), NVL(SLT_CDSTSCRCD,'NA!@#'), NVL(SLT_CMODE,'NA!@#'), NVL(SLT_CPRODTYPE,'NA!@#'), NVL(SLT_CTRNCODE,'NA!@#'), NVL(SLT_CFOCCODE,'NA!@#'), NVL(SLT_BPRIORITY,'NA!@#'), NVL(SLT_CCUSTCODE,'NA!@#'), NVL(SLT_CSENDER,'NA!@#'), NVL(SLT_CCNEECODE,'NA!@#'), NVL(SLT_CATTENTION,'NA!@#'), NVL(SLT_NPCS,'NA!@#'), NVL(SLT_NWEIGHT,'NA!@#'), NVL(SLT_NACTWGT,'NA!@#'), NVL(SLT_CCMDTYCODE,'NA!@#'), NVL(SLT_NTOKENNO,'NA!@#'), NVL(SLT_NCSHMEMNO,'NA!@#'), NVL(SLT_NAMT,'NA!@#'), NVL(SLT_CBILLNO,'NA!@#'), NVL(SLT_BBILLCNEE,'NA!@#'), NVL(SLT_CBILLCAC,'NA!@#'), NVL(SLT_CPACKTYPE,'NA!@#'), NVL(SLT_DSHIPDATE,'NA!@#'), NVL(SLT_DPUDATE,'NA!@#'), NVL(SLT_CPUTIME,'NA!@#'), NVL(SLT_CPUEMPLCD,'NA!@#'), NVL(SLT_CPUWI,'NA!@#'), NVL(SLT_DEPTDTDLV,'NA!@#'), NVL(SLT_CODA,'NA!@#'), NVL(SLT_CFLIGHTNO,'NA!@#'), NVL(SLT_DFLIGHTDT,'NA!@#'), NVL(SLT_CKGPOUND,'NA!@#'), NVL(SLT_BLINKED,'NA!@#'), NVL(SLT_CADECODE,'NA!@#'), NVL(SLT_CCRCRDREF,'NA!@#'), NVL(SLT_NDIML,'NA!@#'), NVL(SLT_NDIMB,'NA!@#'), NVL(SLT_NDIMH,'NA!@#'), NVL(SLT_NSLABWGT,'NA!@#'), NVL(SLT_NASSDVALUE,'NA!@#'), NVL(SLT_CDOCNO,'NA!@#'), NVL(SLT_DDOCDATE,'NA!@#'), NVL(SLT_CPAYTYPE,'NA!@#'), NVL(SLT_NAMOUNT,'NA!@#'), NVL(SLT_CINVNO,'NA!@#'), NVL(SLT_DINVDATE,'NA!@#'), NVL(SLT_NOTHCHRGS,'NA!@#'), NVL(SLT_NCDUTYPC,'NA!@#'), NVL(SLT_CCRCARDNO,'NA!@#'), NVL(SLT_CCARDCODE,'NA!@#'), NVL(SLT_CCARDHOLD,'NA!@#'), NVL(SLT_DVALIDUPTO,'NA!@#'), NVL(SLT_CCUSTNAME,'NA!@#'), NVL(SLT_CCUSTADR1,'NA!@#'), NVL(SLT_CCUSTADR2,'NA!@#'), NVL(SLT_CCUSTADR3,'NA!@#'), NVL(SLT_CCUSTPIN,'NA!@#'), NVL(SLT_CCUSTTEL,'NA!@#'), NVL(SLT_CCUSTFAX,'NA!@#'), NVL(SLT_CCNEENAME,'NA!@#'), NVL(SLT_CCNEEADR1,'NA!@#'), NVL(SLT_CCNEEADR2,'NA!@#'), NVL(SLT_CCNEEADR3,'NA!@#'), NVL(SLT_CCNEEPIN,'NA!@#'), NVL(SLT_CCNEETEL,'NA!@#'), NVL(SLT_CCNEEFAX,'NA!@#'), NVL(SLT_BCHECKLST,'NA!@#'), NVL(SLT_CSPLINST,'NA!@#'), NVL(SLT_CPRODDESC,'NA!@#'), NVL(SLT_DBATCHDT,'NA!@#'), NVL(SLT_NOCTROI,'NA!@#'), NVL(SLT_CCLECTYPE,'NA!@#'), NVL(SLT_NDCLRDVAL,'NA!@#'), NVL(SLT_BSTAMPDART,'NA!@#'), NVL(SLT_CCMDTYDESC,'NA!@#'), NVL(SLT_CCALTNAME,'NA!@#'), NVL(SLT_CALTATTN,'NA!@#'), NVL(SLT_CCALTADR1,'NA!@#'), NVL(SLT_CCALTADR2,'NA!@#'), NVL(SLT_CCALTADR3,'NA!@#'), NVL(SLT_CCALTPIN,'NA!@#'), NVL(SLT_CCALTTEL,'NA!@#'), NVL(SLT_CCALTFAX,'NA!@#'), NVL(SLT_CCNEEMOB,'NA!@#'), NVL(SLT_CCALTMOB,'NA!@#'), NVL(SLT_NCOLAMT,'NA!@#'), NVL(SLT_CFODCODFLG,'NA!@#'), NVL(SLT_CSUBCODE,'NA!@#'), NVL(SLT_CCTMNO,'NA!@#'), NVL(SLT_BDOXATCHD,'NA!@#'), NVL(SLT_CMRKSNOS1,'NA!@#'), NVL(SLT_CMRKSNOS2,'NA!@#'), NVL(SLT_CMRKSNOS3,'NA!@#'), NVL(SLT_CDIMEN1,'NA!@#'), NVL(SLT_CDIMEN2,'NA!@#'), NVL(SLT_CDIMEN3,'NA!@#'), NVL(SLT_NCHRGWT,'NA!@#'), NVL(SLT_NCOMVAL,'NA!@#'), NVL(SLT_NFREIGHT,'NA!@#'), NVL(SLT_NVALCHGS,'NA!@#'), NVL(SLT_NAWBFEE,'NA!@#'), NVL(SLT_CAWBFEE,'NA!@#'), NVL(SLT_NSTATCHG,'NA!@#'), NVL(SLT_CSTATCHG,'NA!@#'), NVL(SLT_NCARTCHG,'NA!@#'), NVL(SLT_CCARTCHG,'NA!@#'), NVL(SLT_NREGTCHG,'NA!@#'), NVL(SLT_CREGTCHG,'NA!@#'), NVL(SLT_NMISCCHG1,'NA!@#'), NVL(SLT_CMISCCHG1,'NA!@#'), NVL(SLT_NMISCCHG2,'NA!@#'), NVL(SLT_CMISCCHG2,'NA!@#'), NVL(SLT_NCHGCOLCT,'NA!@#'), NVL(SLT_COCTRCPTNO,'NA!@#'), NVL(SLT_CEMAILID,'NA!@#'), NVL(SLT_CBTPAREA,'NA!@#'), NVL(SLT_CBTPCODE,'NA!@#'), NVL(SLT_CCODFAVOUR,'NA!@#'), NVL(SLT_CCODPAYBLE,'NA!@#'), NVL(SLT_CFOVTYPE,'NA!@#'), NVL(SLT_NADDISRCHG,'NA!@#'), NVL(SLT_NADDOSRCHG,'NA!@#'), NVL(SLT_NDOCCHRG,'NA!@#'), NVL(SLT_NDCCHRG,'NA!@#'), NVL(SLT_NFODCHRG,'NA!@#'), NVL(SLT_NRISKHCHGS,'NA!@#'), NVL(SLT_NODACHRG,'NA!@#'), NVL(SLT_CGSACODE,'NA!@#'), NVL(SLT_NFSAMT,'NA!@#'), NVL(SLT_CDHLFLAG,'NA!@#'), NVL(SLT_NDODAMT,'NA!@#'), NVL(SLT_CLOCCODE,'NA!@#'), NVL(SLT_CILLOC,'NA!@#'), NVL(SLT_CDLCODE,'NA!@#'), NVL(SLT_NDIWT,'NA!@#'), NVL(SLT_CCUSTMOB,'NA!@#'), NVL(SLT_CADNLCRCRDREF,'NA!@#'), NVL(SLT_DUPLDATE,'NA!@#'), NVL(SLT_CREGCUSTNAME,'NA!@#'), NVL(SLT_CREGCUSTADR1,'NA!@#'), NVL(SLT_CREGCUSTADR2,'NA!@#'), NVL(SLT_CREGCUSTADR3,'NA!@#'), NVL(SLT_CREGCUSTPIN,'NA!@#'), NVL(SLT_CREGCUSTTEL,'NA!@#'), NVL(SLT_CRTOIMDTLY,'NA!@#'), NVL(SLT_CPREFTM,'NA!@#'), NVL(SLT_CREVPU,'NA!@#'), NVL(SLT_CPSCODE,'NA!@#'), NVL(SLT_DDWNLDDATE,'NA!@#'), NVL(SLT_CFRDAWBNO,'NA!@#'), NVL(SLT_CRFDCOMPNM,'NA!@#'), NVL(SLT_CREFNO2,'NA!@#'), NVL(SLT_CREFNO3,'NA!@#'), NVL(SLT_CPUMODE,'NA!@#'), NVL(SLT_CPUTYPE,'NA!@#'), NVL(SLT_NITEMCNT,'NA!@#'), NVL(SLT_BPARTIALPU,'NA!@#'), NVL(SLT_NPAYCASH,'NA!@#'), NVL(SLT_NPUTMSLOT,'NA!@#'), NVL(SLT_CCNEEMAIL,'NA!@#'), NVL(SLT_CRTOCONTNM,'NA!@#'), NVL(SLT_CRTOADR1,'NA!@#'), NVL(SLT_CRTOADR2,'NA!@#'), NVL(SLT_CRTOADR3,'NA!@#'), NVL(SLT_CRTOPIN,'NA!@#'), NVL(SLT_CRTOTEL,'NA!@#'), NVL(SLT_CRTOMOB,'NA!@#'), NVL(SLT_CMANIFSTNO,'NA!@#'), NVL(SLT_CRTOLAT,'NA!@#'), NVL(SLT_CRTOLON,'NA!@#'), NVL(SLT_CRTOADRDT,'NA!@#'), NVL(SLT_CCUSTLAT,'NA!@#'), NVL(SLT_CCUSTLON,'NA!@#'), NVL(SLT_CCUSTADRDT,'NA!@#'), NVL(SLT_CCNEELAT,'NA!@#'), NVL(SLT_CCNEELON,'NA!@#'), NVL(SLT_CCNEEADRDT,'NA!@#'), NVL(SLT_CCALTLAT,'NA!@#'), NVL(SLT_CCALTLON,'NA!@#'), NVL(SLT_CCALTADRDT,'NA!@#'), NVL(SLT_CREGCUSTLAT,'NA!@#'), NVL(SLT_CREGCUSTLON,'NA!@#'), NVL(SLT_CREGCUSTADRDT,'NA!@#'), NVL(SLT_COFFCLTIME,'NA!@#'), NVL(SLT_NDEFERREDDELIVERYDAYS,'NA!@#'), NVL(SLT_CFLFM,'NA!@#'), NVL(SLT_CCNTRYCODE,'NA!@#'), NVL(SLT_CSTATECODE,'NA!@#'), NVL(SLT_DCUSTEDD,'NA!@#'), NVL(SLT_CISDDN,'NA!@#'), NVL(SLT_CACTDELLOC,'NA!@#'), NVL(SLT_CGSTNO,'NA!@#'), NVL(SLT_CCUSTGSTNO,'NA!@#'), NVL(SLT_CCNEEGSTNO,'NA!@#'), NVL(SLT_CEXCHAWB,'NA!@#'), NVL(SLT_CCNEEFADD,'NA!@#'), NVL(SLT_DCUSTPUDT,'NA!@#'), NVL(SLT_CCUSTPUTM,'NA!@#'), NVL(SLT_CCONMOBMASK,'NA!@#'), NVL(SLT_CADDRESSTYPE,'NA!@#'), NVL(SLT_CAVAILTIME,'NA!@#'), NVL(SLT_CAVAILDAYS,'NA!@#'), NVL(SLT_CCSTMOBMAS,'NA!@#'), NVL(SLT_CRTOMOBMAS,'NA!@#'), NVL(SLT_COTPTYPE,'NA!@#'), NVL(SLT_NOTPNO,'NA!@#'), NVL(SLT_CINCOTERMS,'NA!@#'), NVL(SLT_CPURTCD,'NA!@#'), NVL(SLT_CPULOC,'NA!@#'), NVL(SLT_CCOMPGRP,'NA!@#'), NVL(SLT_CPACKAGINGID,'NA!@#'), NVL(SLT_OP_TS,'NA!@#'), NVL(SLT_OP_TYPE,'NA!@#'), NVL(CUST_ccompgrp,'NA!@#'), NVL(PACK_cprodparent,'NA!@#'), NVL(PACK_cprodcommname,'NA!@#'), NVL(PACK_csubprdcommname,'NA!@#'), NVL(PACK_cprodgroup,'NA!@#'), NVL(cgrpname,'NA!@#') ) as CT_COL from sltpack where SLT_CAWBNO!='D!@#' AND (SLT_CAWBNO!='NA!@#' or SLT_CAWBNO is not null)""")

        val shipclt_dfPwriteDel = sqlContext.sql(""" SELECT NVL(TRIM(PK),'NA!@#') AS PK, 'D!@#' AS CT_COL from sltpack where SLT_CAWBNO = 'D!@#' AND (SLT_CAWBNO!='NA!@#' or SLT_CAWBNO is not null)""")
        
        shipcltDF = shipclt_dfPwriteDel.unionAll(shipcltDF_nondelete)


         podJourney = sqlContext.sql("""select NVL(TRIM(PK),'NA!@#') as PK,NVL(journey,'NA!@#') as CT_COL from finalpod""")

        sfmJourney = sqlContext.sql("""select NVL(TRIM(PK),'NA!@#') as PK,NVL(journey,'NA!@#') as CT_COL from finalsfj""")

         mdpickup_DF = sqlContext.sql(""" select NVL(TRIM(New_RowKey),'NA!@#') as PK,CONCAT_WS('~',NVL(MDP_CAWBNO,'NA!@#'), NVL(MDP_CPICKUPID,'NA!@#'), NVL(MDP_CPRODCODE,'NA!@#'), NVL(MDP_CCUSTCODE,'NA!@#'), NVL(MDP_CORGAREA,'NA!@#'), NVL(MDP_CORGSCRCD,'NA!@#'), NVL(MDP_CDSTAREA,'NA!@#'), NVL(MDP_CDSTSCRCD,'NA!@#'), NVL(MDP_CSUBCODE,'NA!@#'), NVL(MDP_CSTATCODE,'NA!@#'), NVL(MDP_DSTATUSDATE,'NA!@#'), NVL(MDP_CMLOCCODE,'NA!@#'), NVL(MDP_CEMPLCODE,'NA!@#'), NVL(MDP_CDEVICENO,'NA!@#'), NVL(MDP_CSIMNO,'NA!@#'), NVL(MDP_CGPSLAT,'NA!@#'), NVL(MDP_CGPSLON,'NA!@#'), NVL(MDP_CGPSTIME,'NA!@#'), NVL(MDP_CGPSSATCNT,'NA!@#'), NVL(MDP_DUPLDDT,'NA!@#'), NVL(MDP_DSYNCDATE,'NA!@#'), NVL(MDP_CSTATUS,'NA!@#'), NVL(MDP_CPURTCODE,'NA!@#'), NVL(MDP_CISPARTIALPICKUP,'NA!@#'), NVL(MDP_CREASONOFPICKUPREJECTION,'NA!@#'), NVL(MDP_CSTATUSTYPE,'NA!@#'), NVL(MDP_CISPICKUPCANCELLED,'NA!@#'), NVL(MDP_NPUTMSLOT,'NA!@#'), NVL(MDP_CTYPE,'NA!@#'), NVL(MDP_DNPUDATE,'NA!@#'), NVL(MDP_CMDPICKUPDETID,'NA!@#'), NVL(MDP_CREMARKS,'NA!@#'), NVL(MDP_OP_TS,'NA!@#'), NVL(MDP_OP_TYPE,'NA!@#') ) as CT_COL from pldf where MDP_CAWBNO!='NA!@#' or MDP_CAWBNO is not null""")
        
         mdpod_DF = sqlContext.sql(""" select NVL(TRIM(New_RowKey),'NA!@#') as PK,CONCAT_WS('~',NVL(MOD_CAWBNO,'NA!@#'), NVL(MOD_CMPODID,'NA!@#'), NVL(MOD_CMPODDETID,'NA!@#'), NVL(MOD_CPRODCODE,'NA!@#'), NVL(MOD_CORGAREA,'NA!@#'), NVL(MOD_CDSTAREA,'NA!@#'), NVL(MOD_CSTATTYPE,'NA!@#'), NVL(MOD_CSTATCODE,'NA!@#'), NVL(MOD_DSTATDATE,'NA!@#'), NVL(MOD_DSTATTIME,'NA!@#'), NVL(MOD_CEMPLCODE,'NA!@#'), NVL(MOD_CRECDBY,'NA!@#'), NVL(MOD_CDSTSCRCD,'NA!@#'), NVL(MOD_CRELATION,'NA!@#'), NVL(MOD_CREMARKS,'NA!@#'), NVL(MOD_CIDTYPE,'NA!@#'), NVL(MOD_CIDNO,'NA!@#'), NVL(MOD_CDEVICENO,'NA!@#'), NVL(MOD_CSIMNO,'NA!@#'), NVL(MOD_CGPSLAT,'NA!@#'), NVL(MOD_CGPSLON,'NA!@#'), NVL(MOD_DTRACK_INSERT,'NA!@#'), NVL(MOD_DTRACK_UPDATE,'NA!@#'), NVL(MOD_CSTATTIME,'NA!@#'), NVL(MOD_CAREA,'NA!@#'), NVL(MOD_CLOCCODE,'NA!@#'), NVL(MOD_DEDPUPDDT,'NA!@#'), NVL(MOD_DSYNCDATE,'NA!@#'), NVL(MOD_CSTATUS,'NA!@#'), NVL(MOD_CGPSTIME,'NA!@#'), NVL(MOD_CGPSSATCNT,'NA!@#'), NVL(MOD_OP_TS,'NA!@#'), NVL(MOD_OP_TYPE,'NA!@#')) as CT_COL from pldf where MOD_CAWBNO!='NA!@#' or MOD_CAWBNO is not null""")
        
         sfm_DF = sqlContext.sql(""" select NVL(TRIM(New_RowKey),'NA!@#') as PK,CONCAT_WS('~',NVL(SFM_CAWBNO,'NA!@#'), NVL(SFM_CMPSNO,'NA!@#'), NVL(SFM_NRUNID,'NA!@#'), NVL(SFM_DRUNDATE,'NA!@#'), NVL(SFM_CRUNCODE,'NA!@#'), NVL(SFM_CORGAREA,'NA!@#'), NVL(SFM_CSTATTYPE,'NA!@#'), NVL(SFM_CSTATCODE,'NA!@#'), NVL(SFM_CEMPLCODE,'NA!@#'), NVL(SFM_CLOCCODE,'NA!@#'), NVL(SFM_NPARRENTCONTID,'NA!@#'), NVL(SFM_DDTINSCAN,'NA!@#'), NVL(SFM_CTALLY,'NA!@#'), NVL(SFM_BPRINTED,'NA!@#'), NVL(SFM_DSTATDATE,'NA!@#'), NVL(SFM_ISUNDELSHP,'NA!@#'), NVL(SFM_ISUNDELIVERDATE,'NA!@#'), NVL(SFM_ISNOTOUTSCAN,'NA!@#'), NVL(SFM_ISNOTOUTSCANDATE,'NA!@#'), NVL(SFM_CRECDATA,'NA!@#'), NVL(SFM_DOUTSCANDT,'NA!@#'), NVL(SFM_BOUTSCAN,'NA!@#'), NVL(SFM_BEDPTRNX,'NA!@#'), NVL(SFM_CDSTAREA,'NA!@#'), NVL(SFM_CPTALLY,'NA!@#'), NVL(SFM_NRPCS,'NA!@#'), NVL(SFM_CDOCATTACH,'NA!@#'), NVL(SFM_NOPCS,'NA!@#'), NVL(SFM_CFLIGHTNO,'NA!@#'), NVL(SFM_DFLIGHTDT,'NA!@#'), NVL(SFM_DLOADARRDATE,'NA!@#'), NVL(SFM_DFLTARRDATE,'NA!@#'), NVL(SFM_CVEHICLENO,'NA!@#'), NVL(SFM_NENDKMS,'NA!@#'), NVL(SFM_DPTALLYDATE,'NA!@#'), NVL(SFM_CACTFLIGHTNO,'NA!@#'), NVL(SFM_DACTFLIGHTDT,'NA!@#'), NVL(SFM_CTALLYSRC,'NA!@#'), NVL(SFM_DDSTARRDT,'NA!@#'), NVL(SFM_CDLVPURTCD,'NA!@#'), NVL(SFM_CTALLYPURTCD,'NA!@#'), NVL(SFM_NACTPARRENTCONTID,'NA!@#'), NVL(SFM_CSTATEMPLCODE,'NA!@#'), NVL(SFM_CSTATCLRACTION,'NA!@#'), NVL(SFM_CSTATCLREMPLCODE,'NA!@#'), NVL(SFM_DSTATCLRDATE,'NA!@#'), NVL(SFM_CMSTATCODE,'NA!@#'), NVL(SFM_OP_TS,'NA!@#'), NVL(SFM_OP_TYPE,'NA!@#')) as CT_COL from pldf where SFM_CAWBNO!='NA!@#' or SFM_CAWBNO is not null""")

         dls_DF = sqlContext.sql("""select NVL(TRIM(New_RowKey),'NA!@#') as PK, NVL(delivery_status,'NA!@#') as CT_COL from pldf""")

        val awbmst_maxts = sqlContext.sql("select max(ABS_LOADTS) AS MAXLTS from awbpack where ABS_CAWBNO!='NA!@#' or ABS_CAWBNO is not null")

        val shipclt_maxts = sqlContext.sql("select max(SLT_LOADTS) AS MAXLTS from sltpack where SLT_CAWBNO!='NA!@#' or SLT_CAWBNO is not null")

        val inb_maxts = sqlContext.sql("select max(IBS_LOADTS) AS MAXLTS from pldf where IBS_CAWBNO!='NA!@#' or IBS_CAWBNO is not null")

        val out_maxts = sqlContext.sql("select max(OBS_LOADTS) AS MAXLTS from pldf where OBS_CAWBNO!='NA!@#' or OBS_CAWBNO is not null")

        val mdpk_maxts = sqlContext.sql("select max(MDP_LOADTS) AS MAXLTS from pldf where MDP_CAWBNO!='NA!@#' or MDP_CAWBNO is not null")

        val mod_maxts = sqlContext.sql("select max(MOD_LOADTS) AS MAXLTS from pldf where MOD_CAWBNO!='NA!@#' or MOD_CAWBNO is not null")

        val sfm_maxts = sqlContext.sql("select max(SFM_LOADTS) AS MAXLTS from pldf where SFM_CAWBNO!='NA!@#' or SFM_CAWBNO is not null")


        val max_TS1 = awbmst_maxts.unionAll(shipclt_maxts).unionAll(inb_maxts).unionAll(out_maxts).unionAll(mdpk_maxts).unionAll(mod_maxts).unionAll(sfm_maxts).agg(max(col("MAXLTS"))).collect()(0)(0).toString()

        /** start changes - convert date format "yyyyMMddHHmmss" to required date format "yyyy-MM-dd HH:mm:ss" **/

        val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
        val date = simpleDateFormat.parse(max_TS1)
        val newDtFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        val max_TS = newDtFmt.format(date)

        /** End changes - convert date format "yyyyMMddHHmmss"statmst_GG_DFto required date format "yyyy-MM-dd HH:mm:ss" **/

         ldts_DF = sqlContext.sql(s"""select NVL(TRIM(New_RowKey),'NA!@#') as PK, '$max_TS' as CT_COL from pldf""")

        if(awbmstDF== null){
          println("awbmst_df is null, creating empty dataframe")
          awbmstDF = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }
        if(inboundDF== null){
          println("inboundDF is null, creating empty dataframe")
          inboundDF = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }
        if(outboundDF == null){
          println("outboundDF is null, creating empty dataframe")
          outboundDF = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }
        if(shipcltDF== null){
          println("shipcltDF is null, creating empty dataframe")
          shipcltDF = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }
        if(podJourney== null){
          println("podJourney is null, creating empty dataframe")
          podJourney = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }
        if(mdpickup_DF== null){
          println("mdpickup_DF is null, creating empty dataframe")
          mdpickup_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }
        if(mdpod_DF== null){
          println("mdpod_DF is null, creating empty dataframe")
          mdpod_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }

        if(ldts_DF== null){
          println("ldts_DF is null, creating empty dataframe")
          ldts_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }

        if(sfm_DF== null){
          println("sfm_DF is null, creating empty dataframe")
          sfm_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }
        if(dls_DF == null){
          println("dls_DF is null, creating empty dataframe")
          dls_DF = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }
        if(delivery_date == null){
          println("delivery_date is null, creating empty dataframe")
          delivery_date = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }

        if(sfmJourney == null){
          println("sfmJourney is null, creating empty dataframe")
          sfmJourney = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
        }


        val pl_map = Map("ABM" -> awbmstDF, "IBS" -> inboundDF, "OBS" -> outboundDF, "SLT" -> shipcltDF, "PDJ" -> podJourney, "MDP" -> mdpickup_DF,"MOD" -> mdpod_DF, "DLS" -> dls_DF, "LTS" -> ldts_DF, "DTS" -> delivery_date,"SFM" -> sfm_DF,"SFJ" -> sfmJourney)

        // Use below pl_map code if DLS and DTS is calculated separately using the logic at the end

       //val pl_map = Map("ABM" -> awbmstDF, "IBS" -> inboundDF, "OBS" -> outboundDF, "SLT" -> shipcltDF, "PDJ" -> podJourney, "MDP" -> mdpickup_DF,"MOD" -> mdpod_DF, "LTS" -> ldts_DF,"SFM" -> sfm_DF)

        println("PL Table dump start::::: " + sdf.format(new Date()))
        pl_map.foreach { a: (String, DataFrame) =>
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
        
        val btchCntrl_DF = sc.parallelize(List(max_TS)).toDF("PL").withColumn("PK",lit("1"))

        def batch_catalog = s"""{
                           |"table":{"namespace":"default", "name":"${BATCH_CNTRL}"},
                           |"rowkey":"PK",
                           |"columns":{
                           |"PK":{"cf":"rowkey", "col":"PK", "type":"string"},
                           |"PL":{"cf":"LTS", "col":"PL", "type":"string"}
                           |}
                           |}""".stripMargin

        println("catalog created :::: " + batch_catalog)

        println(sdf.format(new Date()))

        btchCntrl_DF.write.options(Map(HBaseTableCatalog.tableCatalog -> batch_catalog, HBaseTableCatalog.newTable -> "5")).format("org.apache.spark.sql.execution.datasources.hbase").save()

        println("catalog LTS written to hbase table:::: " + sdf.format(new Date()))


        println("after write to PL Hbase" + sdf.format(new Date()))

        /*
        Code to update DLS and DTS -- added on 22nd July 2019
         */
        //val test_PL = "CTOWER.CT_PL_CAWBNO"
        //config.set(TableInputFormat.INPUT_TABLE, test_PL)
       /* config.set(TableInputFormat.INPUT_TABLE,PL_tableName_AWBNO)

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

        }*/
      }
    })
  }
}

