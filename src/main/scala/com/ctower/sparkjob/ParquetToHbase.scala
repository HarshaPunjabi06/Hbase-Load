package com.ctower.sparkjob

import java.text.SimpleDateFormat

import com.biapps.ctower.hbase.HbaseConnection
import com.ctower.util.schema.SchemaHbase._
import com.ctower.util.ConstantHbase._
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.functions.{collect_list, concat_ws, sort_array, _}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import java.util.Properties
import org.apache.hadoop.fs.{FSDataInputStream,FileSystem, Path}
import org.apache.hadoop.conf.Configuration

object ParquetToHbase {

	val prop = new Properties()
	val hdfsConfig = new Configuration()
	val propFilepath = new Path(PROPFILE)
	val hdfsfileSystem = FileSystem.get(hdfsConfig)
	val hdfsFileOpen = hdfsfileSystem.open(propFilepath)
	prop.load(hdfsFileOpen)

	val conf = new SparkConf().setAppName(prop.getProperty(APP_NAME)).setMaster(prop.getProperty(DEPLOY_MODE))
	conf.set("spark.dynamicAllocation.enabled", prop.getProperty(DYNAMIC_ALLOCATION))
	conf.set("spark.driver.memory", prop.getProperty(DRIVER_MEMORY))
	conf.set("spark.executor.memory", prop.getProperty(EXECUTOR_MEMORY))
	conf.set("spark.executor.cores", prop.getProperty(EXECUTOR_CORE))
	conf.set("spark.executor.instances", prop.getProperty(NUM_EXECUTOR))
	conf.set("spark.speculation", prop.getProperty(SPECULATIVE_EXECUTION))
	conf.set("spark.yarn.maxAppAttempts",prop.getProperty(MAX_ATTEMPTS))
	conf.set("spark.yarn.am.attemptFailuresValidityInterval",prop.getProperty(FAILURE_INTERVAL))
	conf.set("spark.task.maxFailures",prop.getProperty(MAX_FAILURES))
	conf.set("principal", prop.getProperty(KEYTAB_PRINCIPAL))
	conf.set("keytab", prop.getProperty(KEYTAB))
	conf.set("hbase.security.authentication", prop.getProperty(HBASE_AUTH))

  val sc = new SparkContext(conf)
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
  val conf_hadoop = sc.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf_hadoop)
	val logger: Logger = LoggerFactory.getLogger(prop.getProperty(LOGGER_NAME))
	PropertyConfigurator.configure(prop.getProperty(LOG_PROP))

  def main(args: Array[String]) {
		
		Hbase_Processing(sqlContext,sc);
  }
	
  
  def fetchLatestBatch (IntermediatePath : String,FinalPath : String) : Unit = {
		 if(checkHadoopPath(IntermediatePath,fs,logger) == true ){
		  val df_intermediate = sqlContext.read.parquet(IntermediatePath)
		  val max_loadts = df_intermediate.agg(max("load_ts")).collect()(0)(0).toString()
		  val df_final = df_intermediate.filter(df_intermediate("load_ts")=== max_loadts)
		  df_final.write.mode("overwrite").partitionBy("load_ts").parquet(FinalPath)
	  }
  }

	def archiveLastProcessed(tabname: String,batchpath : String ,streampath: String,archivepath: String) : Unit = {

		if(checkHadoopPath(batchpath+ tabname,fs,logger) == true) {
			if(isEmptyPath(batchpath+ tabname) == false){
			val els = sqlContext.read.parquet(batchpath + tabname)
			val lts = els.select(col("load_ts")).distinct.map(r => r.getLong(0)).collect.toList

			for (x <- lts) {

				val path1 = streampath + tabname + "/load_ts=" + x
				val path2 = archivepath + tabname
				val flag = fs.rename(new org.apache.hadoop.fs.Path(path1), new org.apache.hadoop.fs.Path(path2))
				println("Moved File " + path1 + "with flag:" + flag)
			}
		}
		}
	}

	def isEmptyPath(pathname: String) : Boolean = {
		println("Inside isEmptyPath check")
		val files = fs.listStatus(new org.apache.hadoop.fs.Path(pathname))
		val partitions = files.map(_.getPath().toString)
		partitions.isEmpty
	}

	/*def identifyCurrentBatch(StreamingPath : String,FinalPath : String,schema : StructType) : DataFrame = {
		var df_notconnect : DataFrame = null;
		var max_ts : String = null;
		var currentDF : DataFrame = null;
		if(checkHadoopPath(StreamingPath,fs,logger) == true){
			// Check if file is empty
			if(isEmptyPath(StreamingPath) == false){
				// Read Complete File
				val existingDF = sqlContext.read.parquet(StreamingPath);
				// Extract max date from FinalPath
				if(checkHadoopPath(FinalPath,fs,logger) == true){
					if(isEmptyPath(FinalPath) == false){
						println("Inside FinalPath CHeck")
						df_notconnect = sqlContext.read.parquet(FinalPath);
						println("Data is present in Final as well as Streaming Path")
						df_notconnect.registerTempTable("existing")
						max_ts = sqlContext.sql("SELECT max(load_ts) from existing").collect()(0)(0).toString()
						currentDF = existingDF.filter(existingDF("load_ts") < from_unixtime(unix_timestamp() - 2 * 60, "yyyyMMddHHmm") && existingDF("load_ts") > max_ts)

					}
				}
				else {
					println("Inside FinalPath CHeck else")
					currentDF = existingDF.filter(existingDF("load_ts") < from_unixtime(unix_timestamp() - 2 * 60, "yyyyMMddHHmm"))
				}
				if(currentDF.take(1).length == 0){
					currentDF = null;
				}
			}
		}

		return currentDF;
	}
*/
	def identifyCurrentBatch(StreamingPath : String,FinalPath : String,schema : StructType) : DataFrame = {
		var df_notconnect : DataFrame = null;
		var max_ts : String = null;
		var currentDF : DataFrame = null;
		var catch_flag : Boolean = false;
		try{
			println("Inside streaming try block")
			if(checkHadoopPath(StreamingPath,fs,logger) == true){

				val existingDF = sqlContext.read.parquet(StreamingPath);
				if(existingDF.rdd.isEmpty == false){
					// Extract max date from FinalPath
					println("inside If")
					try{
						println("Inside final path try block")
						if(checkHadoopPath(FinalPath,fs,logger) == true){

							println("Inside FinalPath CHeck")
							df_notconnect = sqlContext.read.parquet(FinalPath);
							println("Data is present in Final as well as Streaming Path")
							df_notconnect.registerTempTable("existing")

							max_ts = sqlContext.sql("SELECT max(load_ts) from existing").collect()(0)(0).toString()
							currentDF = existingDF.filter(existingDF("load_ts") < from_unixtime(unix_timestamp() - 2 * 60, "yyyyMMddHHmmss") && existingDF("load_ts") > max_ts)

						}
					}
					catch{
						case ex: Throwable => {
							println("In catch block FINAL PATH")
							catch_flag = true
							currentDF = existingDF.filter(existingDF("load_ts") < from_unixtime(unix_timestamp() - 2 * 60, "yyyyMMddHHmmss"))

						}
					}

				}
			}}
		catch{
			case ex: Throwable => {

				if(catch_flag == false){
					println("In catch block streaming path")
					currentDF = null;
				}}
		}
		try{
			if(currentDF.rdd.isEmpty == true){
				currentDF = null;
			}
		}
		catch{
			case ex: Throwable => {
				println("In catch block last")
				currentDF = null;

			}
		}

		return currentDF;
	}

	def checkHadoopPath(pathname: String, fs: FileSystem, logger: Logger): Boolean = {
    println(">>>>>>>>>>pathname<<<<<<<<<" + pathname)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(pathname));
    println(">>>>>>>>>> exists :>>>>>>>>>>>>" + exists)
    return exists;
  }
  
  def removeHadoopPath(pathname: String, fs: FileSystem, logger: Logger): Unit = {
    println(">>>>>>>>>>pathname<<<<<<<<<" + pathname)
    //remove recursive from the path
	if(checkHadoopPath(pathname,fs,logger)== true){
		fs.delete(new org.apache.hadoop.fs.Path(pathname), true)
    //val exists = fs.exists(new org.apache.hadoop.fs.Path(pathname));
	}
	else{
		println("Path not found")
	}
  }
  
  def Hbase_Processing(sqlContext: HiveContext, sc: SparkContext) : Unit = {
	val dateFmt = "HH:mm:ss"
    val sdf = new SimpleDateFormat(dateFmt)

		/*** Test Load Path ****/
	/*	val tempPath = "/user/datalake/ajangid/HbaseProcessing/HarshaTest/tempPath/"
    val STREAMING_PATH = "/user/datalake/ajangid/HbaseProcessing/TestSampleData/"
    val FINAL_PATH = "/user/datalake/ajangid/HbaseProcessing/HarshaTest/FinalPath/"
    val BATCH_INTERMEDIATE = "/user/datalake/ajangid/HbaseProcessing/HarshaTest/batch_intermediate/"*/

		/*** Incremental Load Path ***/
		val tempPath = prop.getProperty(TEMP_PATH)
		val STREAMING_PATH = prop.getProperty(STREAM_PATH)
		val FINAL_PATH = prop.getProperty(FINALPATH)
		val BATCH_INTERMEDIATE = prop.getProperty(BATCH_INTER)
		val ARCHIVE_PATH = prop.getProperty(ARCHIVEPATH)

		/*** Initial Load Path ***/

	/*	val tempPath = "/user/datalake/ajangid/HbaseProcessing/Test/tempPath/"
    val STREAMING_PATH = "/user/datalake/RawStreamingInitialLoad/"
    val FINAL_PATH = "/user/datalake/ajangid/HbaseProcessing/Test/FinalPath/"
    val BATCH_INTERMEDIATE = "/user/datalake/ajangid/HbaseProcessing/Test/batch_intermediate/"*/

		var awbmst_df:DataFrame = null;
		var inbound_df:DataFrame = null;
		var outbound_df:DataFrame = null;
		var shipclt_df:DataFrame = null;
		var addressdtl_df: DataFrame = null;
		var mdpkp_df: DataFrame = null;
		var callpus_df: DataFrame = null;
		var awblink_df:DataFrame = null;
		var sryaltinst_df:DataFrame = null;
    var inbjourney_df: DataFrame = null;
    var obsjourney_df: DataFrame = null;
    var podjourney_df: DataFrame = null;
    var callpusjourney_df: DataFrame = null;
    var mdpod_df: DataFrame = null;
		var shpfltmst_df: DataFrame = null;
		var callpus_ts:DataFrame=null;
		var shpfltmstjourney_df :DataFrame = null;

    val hbaseConn = new HbaseConnection()
    var custmstdf: DataFrame = null;
    var packmstDF: DataFrame = null;
    var cmgrpmstDF: DataFrame = null;

		val MSTDATA_PATH = prop.getProperty(MST_PRIMARY)
		val MSTBCK_PATH = prop.getProperty(MST_BACKUP)

		//***********************Initial Load**************************
		/*val MSTDATA_PATH ="/user/datalake/ajangid/HbaseProcessing/Test/MasterData/Primary/"
		val MSTBCK_PATH ="/user/datalake/ajangid/HbaseProcessing/Test/MasterData/Backup/"*/

		var statmst_GG_DF:DataFrame =null;
		val StatmstSchema = StructType(
			List(
				StructField("stat_cstatcode", StringType),
				StructField("stat_cstattype", StringType),
				StructField("stat_cstatgroup", StringType),
				StructField("stat_cstatdesc", StringType)))

    val HbaseSchema = StructType(
      List(
        StructField("PK", StringType),
        StructField("CT_COL", StringType)))
		
	val awbmst_current = identifyCurrentBatch(STREAMING_PATH+"awbmstDF_record",FINAL_PATH+"awbmstDF_record",getawbmstSchema(sc))
	if (awbmst_current!=null) {
			awbmst_current.write.mode("overwrite").partitionBy("load_ts").parquet(BATCH_INTERMEDIATE +"awbmstDF_record")
			val awbmst_parquet_DF = sqlContext.read.parquet(BATCH_INTERMEDIATE +"awbmstDF_record")

			val awbmst_df1 = awbmst_parquet_DF.filter(col("op_type") !== "D")

    /** start changes -- Remmoved ~ AND | FROM SOURCE DATA **/

    //val awbmst_df2 = Seq("CSPLINST","CEMAILID","CAREMARKS").foldLeft(awbmst_df1){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","NA!@#")) }

		val awbmst_df2 = Seq("CAWBNO","NAWBID","CPRODCODE","CPRODTYPE","CORGAREA","CDSTAREA","CBATCHNO","NPCS","DSHIPDATE","DPUDATE","NTOKENNO","CPUWI","NDIMWEIGHT","CCUSTCODE","NCSHMEMNO","CCMDTYCODE","NCOLAMT","NAMT","CTRNCODE","CSPLINST","CPACKTYPE","CODA","CMODE","CGSACODE","CFODCODFLG","CFOCCODE","CBTPCODE","CBTPAREA","CBILLNO","BREFNO","BDUTY","CSECTYCK","NPARRENTCONTID","BBILLCNEE","CPUEMPCODE","BSTAMDART","CORGSCRCD","CDSTSCRCD","BPRIORITY","CSUBCODE","NCURLOOKUPID","CPUTIME","CFLFM","CDHLACCTNO","CRTOAREA","CRTOCODE","CEMAILID","NWEIGHT","NOCTROI","NDUTY","NSMARTBOXTYPEA","NSMARTBOXTYPEB","NDCCOUNT","CBILLCAC","BCRCRDREF","NDCRECD","CDLVPURTCD","CACCURACY","CDLCODE","CKGPOUND","CMLOCCODE","CADECODE","DINSCANDT","CINSCANLOC","BEDPTRNX","BCRCRDPAY","BIATADTL","CDOCATTACH","CEMPLCODE","NOPCS","CMUSTGOCRG","CODOCATTAC","CDATAENTRYLOC","BSHPCRCRDREF","DEPTDTDLV","BDETAIN","CISOVERAGE","CDHLFLAG","DDEMUDT","NDEMUAMT","CDEMUCODE","CDEMULOCCODE","DSTATUSDT","BCSBPRINTED","DDATAENTRYDT","DBATCHDT","CASTATTYPE","CASTATCODE","CSTATEMPLCODE","CAREMARKS","CAPTCODE","BPWPALLETIZED","CPRINTMODE","CPROMOCODE","CRTOIMTLY","BDGSHIPMENT","ISPWREC","CPREFTM","CREVPU","CPSCODE","CFRDAWBNO","CRFDCOMPNM","CREFNO2","CREFNO3","CPUMODE","CPUTYPE","NITEMCNT","BPARTIALPU","NPAYCASH","NPUTMSLOT","CMANIFSTNO","COFFCLTIME","NDEFERREDDELIVERYDAYS","DCUSTEDD","CISDDN","CACTDELLOC","CGSTNO","CVEHICLENO","CEXCHAWB","DPREFDATE","CPREFTIME","NDISTANCE","DCUSTPUDT","CCUSTPUTM","CAVAILTIME","CAVAILDAYS","COTPTYPE","NOTPNO","CPACKAGINGID","CCCODE").foldLeft(awbmst_df1){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","")) }

    val awbmst_df3 = awbmst_df2.orderBy("cawbno", "op_ts").registerTempTable("AWB_MST");

    /** END changes -- Remmoved ~ AND | FROM SOURCE DATA **/

		awbmst_df = sqlContext.sql("""SELECT NVL(TRIM(cawbno),'NA!@#') AS PK, CONCAT_WS('~', NVL(TRIM(cawbno),'NA!@#'), NVL(nawbid,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(cprodtype,'NA!@#'), NVL(corgarea,'NA!@#'), NVL(cdstarea,'NA!@#'), NVL(cbatchno,'NA!@#'), NVL(npcs,'NA!@#'), NVL(dshipdate,'NA!@#'), NVL(dpudate,'NA!@#'), NVL(ntokenno,'NA!@#'), NVL(cpuwi,'NA!@#'), NVL(ndimweight,'NA!@#'), NVL(ccustcode,'NA!@#'), NVL(ncshmemno,'NA!@#'), NVL(ccmdtycode,'NA!@#'), NVL(ncolamt,'NA!@#'), NVL(namt,'NA!@#'), NVL(ctrncode,'NA!@#'), NVL(csplinst,'NA!@#'), NVL(cpacktype,'NA!@#'), NVL(coda,'NA!@#'), NVL(cmode,'NA!@#'), NVL(cgsacode,'NA!@#'), NVL(cfodcodflg,'NA!@#'), NVL(cfoccode,'NA!@#'), NVL(cbtpcode,'NA!@#'), NVL(cbtparea,'NA!@#'), NVL(cbillno,'NA!@#'), NVL(brefno,'NA!@#'), NVL(bduty,'NA!@#'), NVL(csectyck,'NA!@#'), NVL(nparrentcontid,'NA!@#'), NVL(bbillcnee,'NA!@#'), NVL(cpuempcode,'NA!@#'), NVL(bstamdart,'NA!@#'), NVL(corgscrcd,'NA!@#'), NVL(cdstscrcd,'NA!@#'), NVL(bpriority,'NA!@#'), NVL(csubcode,'NA!@#'), NVL(ncurlookupid,'NA!@#'), NVL(cputime,'NA!@#'), NVL(cflfm,'NA!@#'), NVL(cdhlacctno,'NA!@#'), NVL(crtoarea,'NA!@#'), NVL(crtocode,'NA!@#'), NVL(cemailid,'NA!@#'), NVL(nweight,'NA!@#'), NVL(noctroi,'NA!@#'), NVL(nduty,'NA!@#'), NVL(nsmartboxtypea,'NA!@#'), NVL(nsmartboxtypeb,'NA!@#'), NVL(ndccount,'NA!@#'), NVL(cbillcac,'NA!@#'), NVL(bcrcrdref,'NA!@#'), NVL(ndcrecd,'NA!@#'), NVL(cdlvpurtcd,'NA!@#'), NVL(caccuracy,'NA!@#'), NVL(cdlcode,'NA!@#'), NVL(ckgpound,'NA!@#'), NVL(cmloccode,'NA!@#'), NVL(cadecode,'NA!@#'), NVL(dinscandt,'NA!@#'), NVL(cinscanloc,'NA!@#'), NVL(bedptrnx,'NA!@#'), NVL(bcrcrdpay,'NA!@#'), NVL(biatadtl,'NA!@#'), NVL(cdocattach,'NA!@#'), NVL(cemplcode,'NA!@#'), NVL(nopcs,'NA!@#'), NVL(cmustgocrg,'NA!@#'), NVL(codocattac,'NA!@#'), NVL(cdataentryloc,'NA!@#'), NVL(bshpcrcrdref,'NA!@#'), NVL(deptdtdlv,'NA!@#'), NVL(bdetain,'NA!@#'), NVL(cisoverage,'NA!@#'), NVL(cdhlflag,'NA!@#'), NVL(ddemudt,'NA!@#'), NVL(ndemuamt,'NA!@#'), NVL(cdemucode,'NA!@#'), NVL(cdemuloccode,'NA!@#'), NVL(dstatusdt,'NA!@#'), NVL(bcsbprinted,'NA!@#'), NVL(ddataentrydt,'NA!@#'), NVL(dbatchdt,'NA!@#'), NVL(castattype,'NA!@#'), NVL(castatcode,'NA!@#'), NVL(cstatemplcode,'NA!@#'), NVL(caremarks,'NA!@#'), NVL(captcode,'NA!@#'), NVL(bpwpalletized,'NA!@#'), NVL(cprintmode,'NA!@#'), NVL(cpromocode,'NA!@#'), NVL(crtoimtly,'NA!@#'), NVL(bdgshipment,'NA!@#'), NVL(ispwrec,'NA!@#'), NVL(cpreftm,'NA!@#'), NVL(crevpu,'NA!@#'), NVL(cpscode,'NA!@#'), NVL(cfrdawbno,'NA!@#'), NVL(crfdcompnm,'NA!@#'), NVL(crefno2,'NA!@#'), NVL(crefno3,'NA!@#'), NVL(cpumode,'NA!@#'), NVL(cputype,'NA!@#'), NVL(nitemcnt,'NA!@#'), NVL(bpartialpu,'NA!@#'), NVL(npaycash,'NA!@#'), NVL(nputmslot,'NA!@#'), NVL(cmanifstno,'NA!@#'), NVL(coffcltime,'NA!@#'), NVL(ndeferreddeliverydays,'NA!@#'), NVL(dcustedd,'NA!@#'), NVL(cisddn,'NA!@#'), NVL(cactdelloc,'NA!@#'), NVL(cgstno,'NA!@#'), NVL(cvehicleno,'NA!@#'), NVL(cexchawb,'NA!@#'), NVL(dprefdate,'NA!@#'), NVL(cpreftime,'NA!@#'), NVL(ndistance,'NA!@#'), NVL(dcustpudt,'NA!@#'), NVL(ccustputm,'NA!@#'), NVL(cavailtime,'NA!@#'), NVL(cavaildays,'NA!@#'), NVL(cotptype,'NA!@#'), NVL(notpno,'NA!@#'), NVL(cpackagingid,'NA!@#'), NVL(cccode,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#'),NVL(load_ts,'NA!@#')) AS CT_COL  from AWB_MST""")

		}
	
	/*val awbaddress_current = identifyCurrentBatch(STREAMING_PATH+"awbaddressdetailsDF_record",FINAL_PATH+"awbaddressdetailsDF_record",getawbadressSchema(sc))

    if (awbaddress_current!=null) {
		awbaddress_current.write.mode("overwrite").partitionBy("load_ts").parquet(BATCH_INTERMEDIATE +"awbaddressdetailsDF_record") 
		val awbaddress_parquet_DF = sqlContext.read.parquet(BATCH_INTERMEDIATE +"awbaddressdetailsDF_record").filter(col("op_type") !== "D")
		awbaddress_parquet_DF.registerTempTable("adttbl")

		val awbaddressdetailsSelfJoin = sqlContext.sql(""" select bdw.op_ts,bdw.nawbid as nawbid, bdw.ccode as ccode, bdw.cattn as csender, bdwn.cattn as cattention from (Select nawbid, cshipper,ccode,op_ts,cattn from (select nawbid, cshipper,ccode,op_ts,cattn, row_number() over(partition by nawbid order by op_ts desc) as rnk from adttbl where cshipper = 'Y') as a where rnk = 1) as bdw join (Select nawbid, cshipper,ccode,op_ts,cattn from (select nawbid, cshipper,ccode,op_ts,cattn, row_number() over(partition by nawbid order by op_ts desc) as rnk from adttbl where cshipper = 'N') as b where rnk = 1) as bdwn
		ON bdw.nawbid = bdwn.nawbid """)

		val adt_df1 = awbaddressdetailsSelfJoin.orderBy("nawbid", "op_ts").registerTempTable("ADT");

		addressdtl_df = sqlContext.sql("""SELECT NVL(nawbid,'NA!@#') AS PK, CONCAT_WS('~', NVL(nawbid,'NA!@#'), NVL(ccode,'NA!@#'), NVL(cattention,'NA!@#'), NVL(csender,'NA!@#'), NVL(op_ts,'NA!@#')) AS CT_COL from ADT""")

 
	}*/

		/**START - STATMST Table Read **/
		try{
			statmst_GG_DF = sqlContext.read.parquet(MSTDATA_PATH+"Ops_CT_Mst_Statmst").select(col("cstatcode").alias("stat_cstatcode"),col("cstattype").alias("stat_cstattype"),col("cstatgroup").alias("stat_cstatgroup"),col("cstatdesc").alias("stat_cstatdesc"))

		}catch{
			case ex: Throwable => {
				statmst_GG_DF = sqlContext.read.parquet(MSTBCK_PATH+"Ops_CT_Mst_Statmst").select(col("cstatcode").alias("stat_cstatcode"),col("cstattype").alias("stat_cstattype"),col("cstatgroup").alias("stat_cstatgroup"),col("cstatdesc").alias("stat_cstatdesc"))
			}
		}
		/**END - STATMST Table Read **/
    
	val inbound_current = identifyCurrentBatch(STREAMING_PATH+"inboundDF_record",FINAL_PATH+"inboundDF_record",getinboundSchema(sc))
    if (inbound_current!=null) {
		inbound_current.write.mode("overwrite").partitionBy("load_ts").parquet(BATCH_INTERMEDIATE +"inboundDF_record") 
		val inboundstatusremarks_GG_DF = sqlContext.read.parquet(BATCH_INTERMEDIATE +"inboundDF_record").filter(col("op_type") !== "D")

      /** start changes -- Remmoved ~ AND | FROM SOURCE DATA **/
	
/*val inbound_df2 = Seq("CREMARKS","CFILENAME").foldLeft(inboundstatusremarks_GG_DF){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","NA!@#")) }*/

val inbound_df2 = Seq("CAWBNO","NSTATUSID","NOPERATIONID","CMPSNO","CSTATCODE","CSTATTYPE","DSTATDATE","CREMARKS","CLOCCODE","CEMPLCODE","CFILENAME","SEDPTRNX","DEDPTRNX","DFILENAME","CPRODCODE","CORGAREA","CDSTAREA","DENTDATE","DLASTMODIFIEDTS").foldLeft(inboundstatusremarks_GG_DF){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","")) }


      /** END changes -- Remmoved ~ AND | FROM SOURCE DATA **/

				/** Start - POD Journey creation at IL **/

			val inb_stat_DF = inbound_df2.join(statmst_GG_DF,inbound_df2("cstattype") === statmst_GG_DF("stat_cstattype") && inbound_df2("cstatcode") === statmst_GG_DF("stat_cstatcode"),"left")
			println("inb stat join")

			inb_stat_DF.registerTempTable("ibsstream")

			inbjourney_df = sqlContext.sql(""" select distinct cawbno AS PK, CONCAT(concat_ws('~',nvl(dstatdate,''),CONCAT(nvl(cstattype,''),nvl(cstatcode,'')), nvl(cloccode,''),nvl( cemplcode,''),nvl(stat_cstatgroup,''),nvl(stat_cstatdesc,'')),"@IB",":",op_type) as journey from ibsstream """)

			/** END - POD Journey creation at IL**/

val inbound_df1 = inbound_df2.orderBy("cawbno", "op_ts").registerTempTable("INB");

		inbound_df = sqlContext.sql(""" SELECT NVL(TRIM(cawbno),'NA!@#') AS PK, CONCAT_WS('~', NVL(TRIM(cawbno),'NA!@#'),  NVL(nstatusid,'NA!@#'), NVL(noperationid,'NA!@#'), NVL(cmpsno,'NA!@#'), NVL(cstatcode,'NA!@#'), NVL(cstattype,'NA!@#'), NVL(dstatdate,'NA!@#'), NVL(cremarks,'NA!@#'), NVL(cloccode,'NA!@#'), NVL(cemplcode,'NA!@#'), NVL(cfilename,'NA!@#'), NVL(sedptrnx,'NA!@#'), NVL(dedptrnx,'NA!@#'), NVL(dfilename,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(corgarea,'NA!@#'), NVL(cdstarea,'NA!@#'), NVL(dentdate,'NA!@#'), NVL(dlastmodifiedts,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#'),NVL(load_ts,'NA!@#'))  AS CT_COL from INB """)

    }
	
	val outbound_current = identifyCurrentBatch(STREAMING_PATH+"outboundDF_record",FINAL_PATH+"outboundDF_record",getoutboundSchema(sc))
    if (outbound_current!=null) {
		outbound_current.write.mode("overwrite").partitionBy("load_ts").parquet(BATCH_INTERMEDIATE +"outboundDF_record")
		val outboundstatusremarks_GG_DF = sqlContext.read.parquet(BATCH_INTERMEDIATE +"outboundDF_record").filter(col("op_type") !== "D")

      /** start changes -- Remmoved ~ AND | FROM SOURCE DATA **/
		
		/*val outbound_df2 = Seq("CREMARKS","CFILENAME").foldLeft(outboundstatusremarks_GG_DF){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","NA!@#")) }*/

		val outbound_df2 = Seq("CAWBNO","NSTATUSID","NOPERATIONID","CPRODCODE","CORGAREA","CDSTAREA","CMPSNO","CSTATCODE","CSTATTYPE","DSTATDATE","CREMARKS","CLOCCODE","CEMPLCODE","CFILENAME","SEDPTRNX","DEDPTRNX","CCNO","DRUNDATE","DLASTMODIFIEDTS").foldLeft(outboundstatusremarks_GG_DF){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","")) }



      /** END changes -- Remmoved ~ AND | FROM SOURCE DATA **/

			/** Start - POD Journey creation at IL **/

			val ob_stat_DF = outbound_df2.join(statmst_GG_DF,outbound_df2("cstattype") === statmst_GG_DF("stat_cstattype") && outbound_df2("cstatcode") === statmst_GG_DF("stat_cstatcode"),"left")

			ob_stat_DF.registerTempTable("obsstream")

			obsjourney_df = sqlContext.sql(""" select distinct cawbno AS PK, CONCAT(concat_ws('~',nvl(dstatdate,''),CONCAT(nvl(cstattype,''),nvl(cstatcode,'')), nvl(cloccode,''),nvl( cemplcode,''),nvl(stat_cstatgroup,''),nvl(stat_cstatdesc,'')),"@OB",":",op_type) as journey from obsstream """)


			/** END - POD Journey creation at IL**/

val outbound_df1 = outbound_df2.orderBy("cawbno", "op_ts").registerTempTable("ONB");

		outbound_df = sqlContext.sql(""" SELECT NVL(TRIM(cawbno),'NA!@#') AS PK, CONCAT_WS('~', NVL(TRIM(cawbno),'NA!@#'),  NVL(nstatusid,'NA!@#'), NVL(noperationid,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(corgarea,'NA!@#'), NVL(cdstarea,'NA!@#'),  NVL(cmpsno,'NA!@#'), NVL(cstatcode,'NA!@#'), NVL(cstattype,'NA!@#'), NVL(dstatdate,'NA!@#'), NVL(cremarks,'NA!@#'), NVL(cloccode,'NA!@#'), NVL(cemplcode,'NA!@#'), NVL(cfilename,'NA!@#'), NVL(sedptrnx,'NA!@#'), NVL(dedptrnx,'NA!@#'), NVL(ccno,'NA!@#'), NVL(drundate,'NA!@#'), NVL(dlastmodifiedts,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#'),NVL(load_ts,'NA!@#')) AS CT_COL from ONB""")


    }
	
	val shipclt_current = identifyCurrentBatch(STREAMING_PATH+"shipcltDF_record",FINAL_PATH+"shipcltDF_record",getShipcltSchema(sc))
    if (shipclt_current!= null) {
	  shipclt_current.write.mode("overwrite").partitionBy("load_ts").parquet(BATCH_INTERMEDIATE +"shipcltDF_record")
      val shipclt_recordDF = sqlContext.read.parquet(BATCH_INTERMEDIATE +"shipcltDF_record")
	  
	  val shipclt_df1 = shipclt_recordDF.filter(col("op_type") !== "D")

      /** start changes -- Remmoved ~ AND | FROM SOURCE DATA **/

     /* val shipcltfold = Seq("CCUSTNAME","CCUSTADR1","CCUSTADR2","CCUSTADR3","CCUSTPIN","CCUSTTEL","CCUSTFAX","CCNEENAME","CCNEEADR1","CCNEEADR2","CCNEEADR3","CCNEEPIN","CCNEETEL","CCNEEFAX","CPRODDESC","CCMDTYDESC","CCALTADR1","CCALTADR2","CCALTADR3","CCALTPIN","CCALTTEL","CCALTFAX","CCNEEMOB","CCALTMOB","CMRKSNOS1","CMRKSNOS2","CMRKSNOS3","CEMAILID","CREGCUSTADR1","CREGCUSTADR2","CREGCUSTADR3","CREGCUSTPIN","CREGCUSTTEL","CCNEEMAIL","CRTOADR1","CRTOADR2","CRTOADR3","CRTOPIN","CRTOTEL","CRTOMOB","CSPLINST","CATTENTION").foldLeft(shipclt_df1){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","NA!@#")) }*/

		 val shipcltfold = Seq("CAWBNO","CBATCHNO","CBATORG","CPRODCODE","CORGAREA","CORGSCRCD","CMAWBNO","CMLOCCODE","CDSTAREA","CDSTSCRCD","CMODE","CPRODTYPE","CTRNCODE","CFOCCODE","BPRIORITY","CCUSTCODE","CSENDER","CCNEECODE","CATTENTION","NPCS","NWEIGHT","NACTWGT","CCMDTYCODE","NTOKENNO","NCSHMEMNO","NAMT","CBILLNO","BBILLCNEE","CBILLCAC","CPACKTYPE","DSHIPDATE","DPUDATE","CPUTIME","CPUEMPLCD","CPUWI","DEPTDTDLV","CODA","CFLIGHTNO","DFLIGHTDT","CKGPOUND","BLINKED","CADECODE","CCRCRDREF","NDIML","NDIMB","NDIMH","NSLABWGT","NASSDVALUE","CDOCNO","DDOCDATE","CPAYTYPE","NAMOUNT","CINVNO","DINVDATE","NOTHCHRGS","NCDUTYPC","CCRCARDNO","CCARDCODE","CCARDHOLD","DVALIDUPTO","CCUSTNAME","CCUSTADR1","CCUSTADR2","CCUSTADR3","CCUSTPIN","CCUSTTEL","CCUSTFAX","CCNEENAME","CCNEEADR1","CCNEEADR2","CCNEEADR3","CCNEEPIN","CCNEETEL","CCNEEFAX","BCHECKLST","CSPLINST","CPRODDESC","DBATCHDT","NOCTROI","CCLECTYPE","NDCLRDVAL","BSTAMPDART","CCMDTYDESC","CCALTNAME","CALTATTN","CCALTADR1","CCALTADR2","CCALTADR3","CCALTPIN","CCALTTEL","CCALTFAX","CCNEEMOB","CCALTMOB","NCOLAMT","CFODCODFLG","CSUBCODE","CCTMNO","BDOXATCHD","CMRKSNOS1","CMRKSNOS2","CMRKSNOS3","CDIMEN1","CDIMEN2","CDIMEN3","NCHRGWT","NCOMVAL","NFREIGHT","NVALCHGS","NAWBFEE","CAWBFEE","NSTATCHG","CSTATCHG","NCARTCHG","CCARTCHG","NREGTCHG","CREGTCHG","NMISCCHG1","CMISCCHG1","NMISCCHG2","CMISCCHG2","NCHGCOLCT","COCTRCPTNO","CEMAILID","CBTPAREA","CBTPCODE","CCODFAVOUR","CCODPAYBLE","CFOVTYPE","NADDISRCHG","NADDOSRCHG","NDOCCHRG","NDCCHRG","NFODCHRG","NRISKHCHGS","NODACHRG","CGSACODE","NFSAMT","CDHLFLAG","NDODAMT","CLOCCODE","CILLOC","CDLCODE","NDIWT","CCUSTMOB","CADNLCRCRDREF","DUPLDATE","CREGCUSTNAME","CREGCUSTADR1","CREGCUSTADR2","CREGCUSTADR3","CREGCUSTPIN","CREGCUSTTEL","CRTOIMDTLY","CPREFTM","CREVPU","CPSCODE","DDWNLDDATE","CFRDAWBNO","CRFDCOMPNM","CREFNO2","CREFNO3","CPUMODE","CPUTYPE","NITEMCNT","BPARTIALPU","NPAYCASH","NPUTMSLOT","CCNEEMAIL","CRTOCONTNM","CRTOADR1","CRTOADR2","CRTOADR3","CRTOPIN","CRTOTEL","CRTOMOB","CMANIFSTNO","CRTOLAT","CRTOLON","CRTOADRDT","CCUSTLAT","CCUSTLON","CCUSTADRDT","CCNEELAT","CCNEELON","CCNEEADRDT","CCALTLAT","CCALTLON","CCALTADRDT","CREGCUSTLAT","CREGCUSTLON","CREGCUSTADRDT","COFFCLTIME","NDEFERREDDELIVERYDAYS","CFLFM","CCNTRYCODE","CSTATECODE","DCUSTEDD","CISDDN","CACTDELLOC","CGSTNO","CCUSTGSTNO","CCNEEGSTNO","CEXCHAWB","CCNEEFADD","DCUSTPUDT","CCUSTPUTM","CCONMOBMASK","CADDRESSTYPE","CAVAILTIME","CAVAILDAYS","CCSTMOBMAS","CRTOMOBMAS","COTPTYPE","NOTPNO","CINCOTERMS","CPURTCD","CPULOC","CCOMPGRP","CPACKAGINGID").foldLeft(shipclt_df1){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","")) }

      val shipclt_df3 = shipcltfold.orderBy("cawbno", "op_ts").registerTempTable("SLT");

      /** END changes -- Remmoved ~ AND | FROM SOURCE DATA **/

	  val shipclt_dfPwrite = sqlContext.sql(""" SELECT NVL(TRIM(cawbno),'NA!@#') AS PK, CONCAT_WS('~',NVL(TRIM(cawbno),'NA!@#'), NVL(cbatchno,'NA!@#'), NVL(cbatorg,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(corgarea,'NA!@#'), NVL(corgscrcd,'NA!@#'), NVL(cmawbno,'NA!@#'), NVL(cmloccode,'NA!@#'), NVL(cdstarea,'NA!@#'), NVL(cdstscrcd,'NA!@#'), NVL(cmode,'NA!@#'), NVL(cprodtype,'NA!@#'), NVL(ctrncode,'NA!@#'), NVL(cfoccode,'NA!@#'), NVL(bpriority,'NA!@#'), NVL(ccustcode,'NA!@#'), NVL(csender,'NA!@#'), NVL(ccneecode,'NA!@#'), NVL(cattention,'NA!@#'), NVL(npcs,'NA!@#'), NVL(nweight,'NA!@#'), NVL(nactwgt,'NA!@#'), NVL(ccmdtycode,'NA!@#'), NVL(ntokenno,'NA!@#'), NVL(ncshmemno,'NA!@#'), NVL(namt,'NA!@#'), NVL(cbillno,'NA!@#'), NVL(bbillcnee,'NA!@#'), NVL(cbillcac,'NA!@#'), NVL(cpacktype,'NA!@#'), NVL(dshipdate,'NA!@#'), NVL(dpudate,'NA!@#'), NVL(cputime,'NA!@#'), NVL(cpuemplcd,'NA!@#'), NVL(cpuwi,'NA!@#'), NVL(deptdtdlv,'NA!@#'), NVL(coda,'NA!@#'), NVL(cflightno,'NA!@#'), NVL(dflightdt,'NA!@#'), NVL(ckgpound,'NA!@#'), NVL(blinked,'NA!@#'), NVL(cadecode,'NA!@#'), NVL(ccrcrdref,'NA!@#'), NVL(ndiml,'NA!@#'), NVL(ndimb,'NA!@#'), NVL(ndimh,'NA!@#'), NVL(nslabwgt,'NA!@#'), NVL(nassdvalue,'NA!@#'), NVL(cdocno,'NA!@#'), NVL(ddocdate,'NA!@#'), NVL(cpaytype,'NA!@#'), NVL(namount,'NA!@#'), NVL(cinvno,'NA!@#'), NVL(dinvdate,'NA!@#'), NVL(nothchrgs,'NA!@#'), NVL(ncdutypc,'NA!@#'), NVL(ccrcardno,'NA!@#'), NVL(ccardcode,'NA!@#'), NVL(ccardhold,'NA!@#'), NVL(dvalidupto,'NA!@#'), NVL(ccustname,'NA!@#'), NVL(ccustadr1,'NA!@#'), NVL(ccustadr2,'NA!@#'), NVL(ccustadr3,'NA!@#'), NVL(ccustpin,'NA!@#'), NVL(ccusttel,'NA!@#'), NVL(ccustfax,'NA!@#'), NVL(ccneename,'NA!@#'), NVL(ccneeadr1,'NA!@#'), NVL(ccneeadr2,'NA!@#'), NVL(ccneeadr3,'NA!@#'), NVL(ccneepin,'NA!@#'), NVL(ccneetel,'NA!@#'), NVL(ccneefax,'NA!@#'), NVL(bchecklst,'NA!@#'), NVL(csplinst,'NA!@#'), NVL(cproddesc,'NA!@#'), NVL(dbatchdt,'NA!@#'), NVL(noctroi,'NA!@#'), NVL(cclectype,'NA!@#'), NVL(ndclrdval,'NA!@#'), NVL(bstampdart,'NA!@#'), NVL(ccmdtydesc,'NA!@#'), NVL(ccaltname,'NA!@#'), NVL(caltattn,'NA!@#'), NVL(ccaltadr1,'NA!@#'), NVL(ccaltadr2,'NA!@#'), NVL(ccaltadr3,'NA!@#'), NVL(ccaltpin,'NA!@#'), NVL(ccalttel,'NA!@#'), NVL(ccaltfax,'NA!@#'), NVL(ccneemob,'NA!@#'), NVL(ccaltmob,'NA!@#'), NVL(ncolamt,'NA!@#'), NVL(cfodcodflg,'NA!@#'), NVL(csubcode,'NA!@#'), NVL(cctmno,'NA!@#'), NVL(bdoxatchd,'NA!@#'), NVL(cmrksnos1,'NA!@#'), NVL(cmrksnos2,'NA!@#'), NVL(cmrksnos3,'NA!@#'), NVL(cdimen1,'NA!@#'), NVL(cdimen2,'NA!@#'), NVL(cdimen3,'NA!@#'), NVL(nchrgwt,'NA!@#'), NVL(ncomval,'NA!@#'), NVL(nfreight,'NA!@#'), NVL(nvalchgs,'NA!@#'), NVL(nawbfee,'NA!@#'), NVL(cawbfee,'NA!@#'), NVL(nstatchg,'NA!@#'), NVL(cstatchg,'NA!@#'), NVL(ncartchg,'NA!@#'), NVL(ccartchg,'NA!@#'), NVL(nregtchg,'NA!@#'), NVL(cregtchg,'NA!@#'), NVL(nmiscchg1,'NA!@#'), NVL(cmiscchg1,'NA!@#'), NVL(nmiscchg2,'NA!@#'), NVL(cmiscchg2,'NA!@#'), NVL(nchgcolct,'NA!@#'), NVL(coctrcptno,'NA!@#'), NVL(cemailid,'NA!@#'), NVL(cbtparea,'NA!@#'), NVL(cbtpcode,'NA!@#'), NVL(ccodfavour,'NA!@#'), NVL(ccodpayble,'NA!@#'), NVL(cfovtype,'NA!@#'), NVL(naddisrchg,'NA!@#'), NVL(naddosrchg,'NA!@#'), NVL(ndocchrg,'NA!@#'), NVL(ndcchrg,'NA!@#'), NVL(nfodchrg,'NA!@#'), NVL(nriskhchgs,'NA!@#'), NVL(nodachrg,'NA!@#'), NVL(cgsacode,'NA!@#'), NVL(nfsamt,'NA!@#'), NVL(cdhlflag,'NA!@#'), NVL(ndodamt,'NA!@#'), NVL(cloccode,'NA!@#'), NVL(cilloc,'NA!@#'), NVL(cdlcode,'NA!@#'), NVL(ndiwt,'NA!@#'), NVL(ccustmob,'NA!@#'), NVL(cadnlcrcrdref,'NA!@#'), NVL(dupldate,'NA!@#'), NVL(cregcustname,'NA!@#'), NVL(cregcustadr1,'NA!@#'), NVL(cregcustadr2,'NA!@#'), NVL(cregcustadr3,'NA!@#'), NVL(cregcustpin,'NA!@#'), NVL(cregcusttel,'NA!@#'), NVL(crtoimdtly,'NA!@#'), NVL(cpreftm,'NA!@#'), NVL(crevpu,'NA!@#'), NVL(cpscode,'NA!@#'), NVL(ddwnlddate,'NA!@#'), NVL(cfrdawbno,'NA!@#'), NVL(crfdcompnm,'NA!@#'), NVL(crefno2,'NA!@#'), NVL(crefno3,'NA!@#'), NVL(cpumode,'NA!@#'), NVL(cputype,'NA!@#'), NVL(nitemcnt,'NA!@#'), NVL(bpartialpu,'NA!@#'), NVL(npaycash,'NA!@#'), NVL(nputmslot,'NA!@#'), NVL(ccneemail,'NA!@#'), NVL(crtocontnm,'NA!@#'), NVL(crtoadr1,'NA!@#'), NVL(crtoadr2,'NA!@#'), NVL(crtoadr3,'NA!@#'), NVL(crtopin,'NA!@#'), NVL(crtotel,'NA!@#'), NVL(crtomob,'NA!@#'), NVL(cmanifstno,'NA!@#'), NVL(crtolat,'NA!@#'), NVL(crtolon,'NA!@#'), NVL(crtoadrdt,'NA!@#'), NVL(ccustlat,'NA!@#'), NVL(ccustlon,'NA!@#'), NVL(ccustadrdt,'NA!@#'), NVL(ccneelat,'NA!@#'), NVL(ccneelon,'NA!@#'), NVL(ccneeadrdt,'NA!@#'), NVL(ccaltlat,'NA!@#'), NVL(ccaltlon,'NA!@#'), NVL(ccaltadrdt,'NA!@#'), NVL(cregcustlat,'NA!@#'), NVL(cregcustlon,'NA!@#'), NVL(cregcustadrdt,'NA!@#'), NVL(coffcltime,'NA!@#'), NVL(ndeferreddeliverydays,'NA!@#'), NVL(cflfm,'NA!@#'), NVL(ccntrycode,'NA!@#'), NVL(cstatecode,'NA!@#'), NVL(dcustedd,'NA!@#'), NVL(cisddn,'NA!@#'), NVL(cactdelloc,'NA!@#'), NVL(cgstno,'NA!@#'), NVL(ccustgstno,'NA!@#'), NVL(ccneegstno,'NA!@#'), NVL(cexchawb,'NA!@#'), NVL(ccneefadd,'NA!@#'), NVL(dcustpudt,'NA!@#'), NVL(ccustputm,'NA!@#'), NVL(cconmobmask,'NA!@#'), NVL(caddresstype,'NA!@#'), NVL(cavailtime,'NA!@#'), NVL(cavaildays,'NA!@#'), NVL(ccstmobmas,'NA!@#'), NVL(crtomobmas,'NA!@#'), NVL(cotptype,'NA!@#'), NVL(notpno,'NA!@#'), NVL(cincoterms,'NA!@#'), NVL(cpurtcd,'NA!@#'), NVL(cpuloc,'NA!@#'), NVL(ccompgrp,'NA!@#'), NVL(cpackagingid,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#'),NVL(load_ts,'NA!@#')) AS CT_COL from SLT """)
	  
	  shipclt_dfPwrite.write.mode("overwrite").parquet(tempPath + "shipclt")
	  
	  val shipclt_df2 = shipclt_recordDF.filter(col("op_type") === "D").orderBy("cawbno", "op_ts").registerTempTable("SLTDelete");
            
	  val shipclt_dfPwriteDel = sqlContext.sql(""" SELECT NVL(TRIM(cawbno),'NA!@#') AS PK, 'D!@#' AS CT_COL from SLTDelete """)
	  shipclt_dfPwriteDel.write.mode("overwrite").parquet(tempPath + "shipclt_delete")

	  val shipclt_dfUpsert = sqlContext.read.parquet(tempPath + "shipclt")
	  val shipclt_dfDelete = sqlContext.read.parquet(tempPath + "shipclt_delete")
	  shipclt_df = shipclt_dfUpsert.unionAll(shipclt_dfDelete)

	  println("shipclt written to parquet")
	}
	val callpus_current = identifyCurrentBatch(STREAMING_PATH+"callpusDF_record",FINAL_PATH+"callpusDF_record",getCallpusSchema(sc))
    if (callpus_current!= null) {
		callpus_current.write.mode("overwrite").partitionBy("load_ts").parquet(BATCH_INTERMEDIATE +"callpusDF_record")
		val callpus_recordDF = sqlContext.read.parquet(BATCH_INTERMEDIATE +"callpusDF_record").filter(col("op_type") !== "D")

      /** start changes -- Remmoved ~ AND | FROM SOURCE DATA **/

		/*val callpus_recordDF1 = Seq("CREMARKS","CCUSTNAME","CCUSTADR1","CCUSTADR2","CCUSTADR3","CCUSTPIN","CCUSTTEL","CCONTACT","CREMARKS","CEMAILID","CREASONREMARK","CCALLERNAME","CCALLERPHONE","CFILENAME").foldLeft(callpus_recordDF){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","NA!@#")) }*/

		val callpus_recordDF1 = Seq("NTOKENNO","CAREA","CSCRCD","DREGDATE","CPRODCODE","CPRODTYPE","CTRNCODE","CCUSTCODE","CCUSTNAME","CCUSTADR1","CCUSTADR2","CCUSTADR3","CCUSTPIN","CCUSTTEL","CCONTACT","DPUDATE","CPUTIME","CPUPSDTM","CREMARKS","NPCS","NWEIGHT","CDLVPURTCD","CTRNXPSD","CPUPSDEMP","BPRINTED","CCUSTFAX","CMOBILENO","CPSDBYEMP","CLOGGEDBY","CREMINDER","DREMDATE","COFFCLTIME","CFOD","CCOD","CODA","BPRIORITY","CALTPURTCD","CPUEMPLCD","CALTPUEMP","NSRNO","CTDD","CMODE","DACTPUDATE","CACTPUTIME","CACTPUEMP","CRESCHPU","CEMAILID","CBOX","CSTATCODE","CSTATTYPE","CALTSCRCD","CREGISTERBY","CMODIFIEDBY","DMODIFIEDDATE","NVOLWEIGHT","CTOPAY","CPUSTATUS","ISLOCKED","LOCKINGUSER","CDTP","CSII","CIMPEXP","NUMBEROFBOX","CREASONREMARK","CATMPTIME","CAWBNO","NEWNTOKENNO","DREASSIGNEDDATE","CCALLERNAME","CCALLERPHONE","DRESCHDATE","BEDPTRNX","CFILENAME","CPUAREA","CISBULKUPLD","CISDEDONE","CREFNO","ETAIL","CRAWBNO","DSTATUSENTRYDT","CREVPU","NATTEMPTED","CCLSTATCODE","CCLSTATTYPE","CCLATMPTIME","CALLSTATUS","CISNEWPU","CISDDN","CGSTNO").foldLeft(callpus_recordDF){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","")) }



      /** END changes -- Remmoved ~ AND | FROM SOURCE DATA **/

callpus_recordDF1.registerTempTable("callpus")

		val cpjor = sqlContext.sql("""select distinct ntokenno AS PK, concat(concat_ws('~',nvl(dregdate,''),nvl(dpudate,''),nvl(crevpu,'')),":",op_type) as journey from callpus""")
		val df_group_test = cpjor.groupBy(cpjor("PK")).agg(sort_array(collect_list(cpjor("journey"))).alias("curr_cp_journey"))

		val cp_test = cpjor.join(df_group_test, cpjor("PK") === df_group_test("PK"), "inner").select(cpjor("PK"),df_group_test("curr_cp_journey").alias("cp_journey"))

		callpusjourney_df = cp_test.withColumn("cp_journey", concat_ws("|", cp_test("cp_journey"))).distinct
		callpusjourney_df = callpusjourney_df.withColumnRenamed("cp_journey","CT_COL")

      /*** start CHanges - Callpus join with custmst and packmst ***/

      val callpus_df1 = callpus_recordDF1.orderBy("ntokenno", "op_ts").registerTempTable("cpsdf_tbl");

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

      custmstdf.registerTempTable("custmstdf")
      packmstDF.registerTempTable("packmstDF")
      cmgrpmstDF.registerTempTable("cmpgrpmstDF")

      val custmst_join_callpus_df_record = sqlContext.sql("""select cps.*,cust.ccompgrp as CUST_ccompgrp from cpsdf_tbl cps left outer join custmstdf cust on cps.CAREA = nvl(cust.CAREA,'NA!@#') and cps.CCUSTCODE = nvl(cust.ccustcode,'NA!@#') """)

      println("After join custmst_join_callpus_df_record ")

      custmst_join_callpus_df_record.registerTempTable("cpscust")

      val compgrpmst_callpus_join_DF = sqlContext.sql("""select n.*,cmp.ccompname as cgrpname
from cpscust n left join cmpgrpmstDF cmp on cmp.ccompgrp=n.CUST_ccompgrp""")

      println("compgrpmst_callpus_join_DF join show")

      compgrpmst_callpus_join_DF.registerTempTable("cpscustcmgrp")

		val callpus_dfPWrite = sqlContext.sql(""" SELECT NVL(ntokenno,'NA!@#') as PK, CONCAT_WS('~',NVL(ntokenno,'NA!@#'), NVL(carea,'NA!@#'), NVL(cscrcd,'NA!@#'), NVL(dregdate,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(cprodtype,'NA!@#'), NVL(ctrncode,'NA!@#'), NVL(ccustcode,'NA!@#'), NVL(ccustname,'NA!@#'), NVL(ccustadr1,'NA!@#'), NVL(ccustadr2,'NA!@#'), NVL(ccustadr3,'NA!@#'), NVL(ccustpin,'NA!@#'), NVL(ccusttel,'NA!@#'), NVL(ccontact,'NA!@#'), NVL(dpudate,'NA!@#'), NVL(cputime,'NA!@#'), NVL(cpupsdtm,'NA!@#'), NVL(cremarks,'NA!@#'), NVL(npcs,'NA!@#'), NVL(nweight,'NA!@#'), NVL(cdlvpurtcd,'NA!@#'), NVL(ctrnxpsd,'NA!@#'), NVL(cpupsdemp,'NA!@#'), NVL(bprinted,'NA!@#'), NVL(ccustfax,'NA!@#'), NVL(cmobileno,'NA!@#'), NVL(cpsdbyemp,'NA!@#'), NVL(cloggedby,'NA!@#'), NVL(creminder,'NA!@#'), NVL(dremdate,'NA!@#'), NVL(coffcltime,'NA!@#'), NVL(cfod,'NA!@#'), NVL(ccod,'NA!@#'), NVL(coda,'NA!@#'), NVL(bpriority,'NA!@#'), NVL(caltpurtcd,'NA!@#'), NVL(cpuemplcd,'NA!@#'), NVL(caltpuemp,'NA!@#'), NVL(nsrno,'NA!@#'), NVL(ctdd,'NA!@#'), NVL(cmode,'NA!@#'), NVL(dactpudate,'NA!@#'), NVL(cactputime,'NA!@#'), NVL(cactpuemp,'NA!@#'), NVL(creschpu,'NA!@#'), NVL(cemailid,'NA!@#'), NVL(cbox,'NA!@#'), NVL(cstatcode,'NA!@#'), NVL(cstattype,'NA!@#'), NVL(caltscrcd,'NA!@#'), NVL(cregisterby,'NA!@#'), NVL(cmodifiedby,'NA!@#'), NVL(dmodifieddate,'NA!@#'), NVL(nvolweight,'NA!@#'), NVL(ctopay,'NA!@#'), NVL(cpustatus,'NA!@#'), NVL(islocked,'NA!@#'), NVL(lockinguser,'NA!@#'), NVL(cdtp,'NA!@#'), NVL(csii,'NA!@#'), NVL(cimpexp,'NA!@#'), NVL(numberofbox,'NA!@#'), NVL(creasonremark,'NA!@#'), NVL(catmptime,'NA!@#'), NVL(TRIM(cawbno),'NA!@#'), NVL(newntokenno,'NA!@#'), NVL(dreassigneddate,'NA!@#'), NVL(ccallername,'NA!@#'), NVL(ccallerphone,'NA!@#'), NVL(dreschdate,'NA!@#'), NVL(bedptrnx,'NA!@#'), NVL(cfilename,'NA!@#'), NVL(cpuarea,'NA!@#'), NVL(cisbulkupld,'NA!@#'), NVL(cisdedone,'NA!@#'), NVL(crefno,'NA!@#'), NVL(etail,'NA!@#'), NVL(crawbno,'NA!@#'), NVL(dstatusentrydt,'NA!@#'), NVL(crevpu,'NA!@#'), NVL(nattempted,'NA!@#'), NVL(cclstatcode,'NA!@#'), NVL(cclstattype,'NA!@#'), NVL(cclatmptime,'NA!@#'), NVL(callstatus,'NA!@#'), NVL(cisnewpu,'NA!@#'), NVL(cisddn,'NA!@#'), NVL(cgstno,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#'),NVL(CUST_ccompgrp,'NA!@#'),NVL(cgrpname,'NA!@#'),NVL(load_ts,'NA!@#')) as CT_COL from cpscustcmgrp""")

      /*** End Changes - Callpus join with custmst and packmst ***/

		callpus_dfPWrite.write.mode("overwrite").parquet(tempPath + "callpus")

		callpus_df = sqlContext.read.parquet(tempPath + "callpus")

		println("callpus written to parquet")
			val cpmax_ts = sqlContext.sql("""select from_unixtime(unix_timestamp(CAST(max(load_ts) AS STRING), "yyyyMMddHHmmss"), "yyyy-MM-dd HH:mm:ss") AS MAXCTS from cpsdf_tbl""").collect()(0)(0).toString()
			callpus_ts = callpus_recordDF.withColumn("CT_COL",lit(cpmax_ts)).select(col("ntokenno").alias("PK"),col("CT_COL"))
			println("Created CTS")


	}



		val mdpickup_current = identifyCurrentBatch(STREAMING_PATH+"mdpickupDF_record",FINAL_PATH+"mdpickupDF_record",getmdpickupSchema(sc))
    if (mdpickup_current!= null) {
		mdpickup_current.write.mode("overwrite").partitionBy("load_ts").parquet(BATCH_INTERMEDIATE +"mdpickupDF_record")
		val mdpickup_recordDF = sqlContext.read.parquet(BATCH_INTERMEDIATE +"mdpickupDF_record").filter(col("op_type") !== "D")
		
		/*val mdpickup_recordDF2 = Seq("CREMARKS").foldLeft(mdpickup_recordDF){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","NA!@#")) }*/

			val mdpickup_recordDF2 = Seq("CAWBNO","CPICKUPID","CPRODCODE","CCUSTCODE","CORGAREA","CORGSCRCD","CDSTAREA","CDSTSCRCD","CSUBCODE","CSTATCODE","DSTATUSDATE","CMLOCCODE","CEMPLCODE","CDEVICENO","CSIMNO","CGPSLAT","CGPSLON","CGPSTIME","CGPSSATCNT","DUPLDDT","DSYNCDATE","CSTATUS","CPURTCODE","CISPARTIALPICKUP","CREASONOFPICKUPREJECTION","CSTATUSTYPE","CISPICKUPCANCELLED","NPUTMSLOT","CTYPE","DNPUDATE","CMDPICKUPDETID","CREMARKS").foldLeft(mdpickup_recordDF){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","")) }


    val mdpkp_df1 = mdpickup_recordDF2.orderBy("cawbno", "op_ts").registerTempTable("mdpkp");
		
		mdpkp_df = sqlContext.sql(""" SELECT NVL(TRIM(cawbno),'NA!@#') AS PK ,CONCAT_WS('~', NVL(TRIM(cawbno),'NA!@#'),NVL(cpickupid,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(ccustcode,'NA!@#'), NVL(corgarea,'NA!@#'), NVL(corgscrcd,'NA!@#'), NVL(cdstarea,'NA!@#'), NVL(cdstscrcd,'NA!@#'), NVL(csubcode,'NA!@#'), NVL(cstatcode,'NA!@#'), NVL(dstatusdate,'NA!@#'), NVL(cmloccode,'NA!@#'), NVL(cemplcode,'NA!@#'), NVL(cdeviceno,'NA!@#'), NVL(csimno,'NA!@#'), NVL(cgpslat,'NA!@#'), NVL(cgpslon,'NA!@#'), NVL(cgpstime,'NA!@#'), NVL(cgpssatcnt,'NA!@#'), NVL(duplddt,'NA!@#'), NVL(dsyncdate,'NA!@#'), NVL(cstatus,'NA!@#'), NVL(cpurtcode,'NA!@#'), NVL(cispartialpickup,'NA!@#'), NVL(creasonofpickuprejection,'NA!@#'), NVL(cstatustype,'NA!@#'), NVL(cispickupcancelled,'NA!@#'), NVL(nputmslot,'NA!@#'), NVL(ctype,'NA!@#'), NVL(dnpudate,'NA!@#'), NVL(cmdpickupdetid,'NA!@#'), NVL(cremarks,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#'),NVL(load_ts,'NA!@#')) AS CT_COL from mdpkp""")
		mdpkp_df.write.mode("overwrite").parquet(tempPath + "mdpkp")
      	}
	
val awblink_current = identifyCurrentBatch(STREAMING_PATH+"awblinkDF_record",FINAL_PATH+"awblinkDF_record",getawblinkSchema(sc))
    if (awblink_current!= null) {
		awblink_current.write.mode("overwrite").partitionBy("load_ts").parquet(BATCH_INTERMEDIATE +"awblinkDF_record")
		val awblink_recordDF = sqlContext.read.parquet(BATCH_INTERMEDIATE +"awblinkDF_record").filter(col("op_type") !== "D")

		val awblink_foldDF1 = Seq("COAWBNO","COORGAREA","CODSTAREA","CNAWBNO","CNORGAREA","CNDSTAREA","NAMT","CPRODCODE","DDATE","DFLIGHTDT","CFLAG","CPRODTYPE","CFLIGHTNO","CLOCCODE","DDFILEDATE").foldLeft(awblink_recordDF){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","")) }


		val awblink_df1 = awblink_foldDF1.orderBy("coawbno", "op_ts").registerTempTable("awblink");

        awblink_df = sqlContext.sql("""SELECT NVL(TRIM(COAWBNO),'NA!@#') AS PK, CONCAT_WS('~', NVL(COAWBNO,'NA!@#'), NVL(COORGAREA,'NA!@#'), NVL(CODSTAREA,'NA!@#'), NVL(CNAWBNO,'NA!@#'), NVL(CNORGAREA,'NA!@#'), NVL(CNDSTAREA,'NA!@#'), NVL(NAMT,'NA!@#'), NVL(CPRODCODE,'NA!@#'), NVL(DDATE,'NA!@#'), NVL(DFLIGHTDT,'NA!@#'), NVL(CFLAG,'NA!@#'), NVL(CPRODTYPE,'NA!@#'), NVL(CFLIGHTNO,'NA!@#'), NVL(CLOCCODE,'NA!@#'), NVL(DDFILEDATE,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#'),NVL(load_ts,'NA!@#')) AS CT_COL from awblink""")

      }
	
	val sryaltinst_current = identifyCurrentBatch(STREAMING_PATH+"sryaltinstDF_record",FINAL_PATH+"sryaltinstDF_record",getsryaltinstSchema(sc))
    if (sryaltinst_current!= null) {
		sryaltinst_current.write.mode("overwrite").partitionBy("load_ts").parquet(BATCH_INTERMEDIATE +"sryaltinstDF_record")
		val sryaltinst_recordDF = sqlContext.read.parquet(BATCH_INTERMEDIATE +"sryaltinstDF_record")
		
		val sryaltinst_df1 = sryaltinst_recordDF.filter(col("op_type") !== "D")
		/*val sryaltinst_df2 = Seq("CREMARKS").foldLeft(sryaltinst_df1){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","NA!@#")) }*/

			val sryaltinst_df2 = Seq("CAWBNO","CTYPE","CEMPLCODE","CTIMEAREA","DATTEMPDT","CEMPLEMAIL","CEMPLNAME","CATTENTION","CNAWBNO","DENTDATE","CENTTIME","CORGAREA","CDSTAREA","CPRODCODE","CCUSTCODE","CMODE","CMAILLOC","CREMARKS","CCUSTNAME","CTYP","BSRYALTINST","CRECDBY","CRELATION","CSTATCODE","CPUWI","DIMPORTDT").foldLeft(sryaltinst_df1){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","")) }


		val sryaltinst_df3 = sryaltinst_df2.orderBy("cawbno", "op_ts").registerTempTable("sryaltinst");

       sryaltinst_df = sqlContext.sql("""SELECT NVL(TRIM(CAWBNO),'NA!@#') AS PK, CONCAT_WS('~',NVL(CAWBNO,'NA!@#'), NVL(CTYPE,'NA!@#'), NVL(CEMPLCODE,'NA!@#'), NVL(CTIMEAREA,'NA!@#'), NVL(DATTEMPDT,'NA!@#'), NVL(CEMPLEMAIL,'NA!@#'), NVL(CEMPLNAME,'NA!@#'), NVL(CATTENTION,'NA!@#'), NVL(CNAWBNO,'NA!@#'), NVL(DENTDATE,'NA!@#'), NVL(CENTTIME,'NA!@#'), NVL(CORGAREA,'NA!@#'), NVL(CDSTAREA,'NA!@#'), NVL(CPRODCODE,'NA!@#'), NVL(CCUSTCODE,'NA!@#'), NVL(CMODE,'NA!@#'), NVL(CMAILLOC,'NA!@#'), NVL(CREMARKS,'NA!@#'), NVL(CCUSTNAME,'NA!@#'), NVL(CTYP,'NA!@#'), NVL(BSRYALTINST,'NA!@#'), NVL(CRECDBY,'NA!@#'), NVL(CRELATION,'NA!@#'), NVL(CSTATCODE,'NA!@#'), NVL(CPUWI,'NA!@#'), NVL(DIMPORTDT,'NA!@#'), NVL(op_ts,'NA!@#'),NVL(op_type,'NA!@#'),NVL(load_ts,'NA!@#')) AS CT_COL from sryaltinst""")

	}
	val mdpod_current = identifyCurrentBatch(STREAMING_PATH+"mdpodDF_record",FINAL_PATH+"mdpodDF_record",getmdpodSchema(sc))
    if (mdpod_current!= null) {
		mdpod_current.write.mode("overwrite").partitionBy("load_ts").parquet(BATCH_INTERMEDIATE +"mdpodDF_record")
		val mdpod_recordDF = sqlContext.read.parquet(BATCH_INTERMEDIATE +"mdpodDF_record")
		
		val mdpod_df1 = mdpod_recordDF.filter(col("op_type") !== "D")

      /*val mdpodfold = Seq("CREMARKS").foldLeft(mdpod_df1){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","NA!@#")) }*/

			val mdpodfold = Seq("CAWBNO","CMPODID","CMPODDETID","CPRODCODE","CORGAREA","CDSTAREA","CSTATTYPE","CSTATCODE","DSTATDATE","DSTATTIME","CEMPLCODE","CRECDBY","CDSTSCRCD","CRELATION","CREMARKS","CIDTYPE","CIDNO","CDEVICENO","CSIMNO","CGPSLAT","CGPSLON","DTRACK_INSERT","DTRACK_UPDATE","CSTATTIME","CAREA","CLOCCODE","DEDPUPDDT","DSYNCDATE","CSTATUS","CGPSTIME","CGPSSATCNT").foldLeft(mdpod_df1){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","")) }


		val mdpod_df3 = mdpodfold.orderBy("cawbno", "op_ts").registerTempTable("mdpod");

			mdpod_df = sqlContext.sql("""SELECT NVL(TRIM(CAWBNO),'NA!@#') AS PK, CONCAT_WS('~',NVL(CAWBNO,'NA!@#'), NVL(CMPODID,'NA!@#'), NVL(CMPODDETID,'NA!@#'), NVL(CPRODCODE,'NA!@#'), NVL(CORGAREA,'NA!@#'), NVL(CDSTAREA,'NA!@#'), NVL(CSTATTYPE,'NA!@#'), NVL(CSTATCODE,'NA!@#'), NVL(DSTATDATE,'NA!@#'), NVL(DSTATTIME,'NA!@#'), NVL(CEMPLCODE,'NA!@#'), NVL(CRECDBY,'NA!@#'), NVL(CDSTSCRCD,'NA!@#'), NVL(CRELATION,'NA!@#'), NVL(CREMARKS,'NA!@#'), NVL(CIDTYPE,'NA!@#'), NVL(CIDNO,'NA!@#'), NVL(CDEVICENO,'NA!@#'), NVL(CSIMNO,'NA!@#'), NVL(CGPSLAT,'NA!@#'), NVL(CGPSLON,'NA!@#'), NVL(DTRACK_INSERT,'NA!@#'), NVL(DTRACK_UPDATE,'NA!@#'), NVL(CSTATTIME,'NA!@#'), NVL(CAREA,'NA!@#'), NVL(CLOCCODE,'NA!@#'), NVL(DEDPUPDDT,'NA!@#'), NVL(DSYNCDATE,'NA!@#'), NVL(CSTATUS,'NA!@#'), NVL(CGPSTIME,'NA!@#'), NVL(CGPSSATCNT,'NA!@#'), NVL(op_ts,'NA!@#'),NVL(op_type,'NA!@#'),NVL(load_ts,'NA!@#')) AS CT_COL from mdpod""")

	}

		val shipfltmst_current = identifyCurrentBatch(STREAMING_PATH+"shipfltmstDF_record",FINAL_PATH+"shipfltmstDF_record",getShipfltmstSchema(sc))
		if (shipfltmst_current!=null) {
			println("inside shipfltmst")

			shipfltmst_current.write.mode("overwrite").partitionBy("load_ts").parquet(BATCH_INTERMEDIATE +"shipfltmstDF_record")
			println("shipfltmst written")

				val shipfltmst_parquet_DF = sqlContext.read.parquet(BATCH_INTERMEDIATE +"shipfltmstDF_record")

			val shipfltmst_df1 = shipfltmst_parquet_DF.filter(col("op_type") !== "D")

        /*val shipfltmstfold = Seq("CRUNCODE").foldLeft(shipfltmst_df1){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","NA!@#")) }*/

			val shipfltmstfold = Seq("cawbno","cmpsno","nrunid","drundate","cruncode","corgarea","cstattype","cstatcode","cemplcode","cloccode","nparrentcontid","ddtinscan","ctally","bprinted","dstatdate","isundelshp","isundeliverdate","isnotoutscan","isnotoutscandate","crecdata","doutscandt","boutscan","bedptrnx","cdstarea","cptally","nrpcs","cdocattach","nopcs","cflightno","dflightdt","dloadarrdate","dfltarrdate","cvehicleno","nendkms","dptallydate","cactflightno","dactflightdt","ctallysrc","ddstarrdt","cdlvpurtcd","ctallypurtcd","nactparrentcontid","cstatemplcode","cstatclraction","cstatclremplcode","dstatclrdate","cmstatcode").foldLeft(shipfltmst_df1){ (df1,colName) => df1.withColumn(colName,regexp_replace(col(colName),"[~|]","")) }


      val shipfltmstfoldr = shipfltmstfold.orderBy("cawbno", "op_ts").registerTempTable("SHPFLT_MST");

			shpfltmst_df = sqlContext.sql("""SELECT NVL(TRIM(cawbno),'NA!@#') AS PK, CONCAT_WS('~', NVL(cawbno,'NA!@#'), NVL(cmpsno,'NA!@#'), NVL(nrunid,'NA!@#'), NVL(drundate,'NA!@#'), NVL(cruncode,'NA!@#'), NVL(corgarea,'NA!@#'), NVL(cstattype,'NA!@#'), NVL(cstatcode,'NA!@#'), NVL(cemplcode,'NA!@#'), NVL(cloccode,'NA!@#'), NVL(nparrentcontid,'NA!@#'), NVL(ddtinscan,'NA!@#'), NVL(ctally,'NA!@#'), NVL(bprinted,'NA!@#'), NVL(dstatdate,'NA!@#'), NVL(isundelshp,'NA!@#'), NVL(isundeliverdate,'NA!@#'), NVL(isnotoutscan,'NA!@#'), NVL(isnotoutscandate,'NA!@#'), NVL(crecdata,'NA!@#'), NVL(doutscandt,'NA!@#'), NVL(boutscan,'NA!@#'), NVL(bedptrnx,'NA!@#'), NVL(cdstarea,'NA!@#'), NVL(cptally,'NA!@#'), NVL(nrpcs,'NA!@#'), NVL(cdocattach,'NA!@#'), NVL(nopcs,'NA!@#'), NVL(cflightno,'NA!@#'), NVL(dflightdt,'NA!@#'), NVL(dloadarrdate,'NA!@#'), NVL(dfltarrdate,'NA!@#'), NVL(cvehicleno,'NA!@#'), NVL(nendkms,'NA!@#'), NVL(dptallydate,'NA!@#'), NVL(cactflightno,'NA!@#'), NVL(dactflightdt,'NA!@#'), NVL(ctallysrc,'NA!@#'), NVL(ddstarrdt,'NA!@#'), NVL(cdlvpurtcd,'NA!@#'), NVL(ctallypurtcd,'NA!@#'), NVL(nactparrentcontid,'NA!@#'), NVL(cstatemplcode,'NA!@#'), NVL(cstatclraction,'NA!@#'), NVL(cstatclremplcode,'NA!@#'), NVL(dstatclrdate,'NA!@#'), NVL(cmstatcode,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#'),NVL(load_ts,'NA!@#')) AS CT_COL  from SHPFLT_MST""")

			println("shpfltmst_df done")


      val shpfltjr = sqlContext.sql("""select distinct cawbno AS PK, concat(concat_ws('~',nvl(NRUNID,''),nvl(DRUNDATE,''),nvl(CORGAREA,''),nvl(CRUNCODE,''),nvl(CSTATTYPE,''),nvl(CSTATCODE,''),nvl(CEMPLCODE,''),nvl(CLOCCODE,''),nvl(DSTATDATE,'')),":",op_type) as journey from SHPFLT_MST where cawbno is not null""")

			val shfl_df_group = shpfltjr.groupBy(shpfltjr("PK")).agg(sort_array(collect_list(shpfltjr("journey"))).alias("curr_shp_journey"))

			val sh_test = shpfltjr.join(shfl_df_group, shpfltjr("PK") === shfl_df_group("PK"), "inner").select(shpfltjr("PK"),shfl_df_group("curr_shp_journey").alias("shflt_journey"))

			shpfltmstjourney_df = sh_test.withColumn("shflt_journey", concat_ws("|", sh_test("shflt_journey"))).distinct
			shpfltmstjourney_df = shpfltmstjourney_df.withColumnRenamed("shflt_journey","CT_COL")

		/*	println("SFJ journey:")
			shpfltmstjourney_df.show(false)*/


		}

		if(inbjourney_df!= null && obsjourney_df!=null){
			println("IBS and OBS Not Null")
			val podunion_df = inbjourney_df.unionAll(obsjourney_df)

			println("After ibs and obs Journey union")

			val df_group_test = podunion_df.groupBy(podunion_df("PK")).agg(sort_array(collect_list(podunion_df("journey"))).alias("curr_pod_journey"))

			val pod_test = podunion_df.join(df_group_test, podunion_df("PK") === df_group_test("PK"), "inner").select(podunion_df("PK"),df_group_test("curr_pod_journey").alias("pod_journey"))

			podjourney_df = pod_test.withColumn("pod_journey", concat_ws("|", pod_test("pod_journey"))).distinct
			val podjourney_dfPwrite = podjourney_df.withColumnRenamed("pod_journey","CT_COL")

			podjourney_dfPwrite.write.mode("overwrite").parquet(tempPath + "podjourney_df")

			podjourney_df = sqlContext.read.parquet(tempPath + "podjourney_df")

			println("podjourney_df written to parquet")

		}
		if(inbjourney_df!= null && obsjourney_df==null){
			println("IBS Not Null and OBS Null")
			val podunion_df = inbjourney_df

			println("After ibs and obs Journey union")

			val df_group_test = podunion_df.groupBy(podunion_df("PK")).agg(sort_array(collect_list(podunion_df("journey"))).alias("curr_pod_journey"))

			val pod_test = podunion_df.join(df_group_test, podunion_df("PK") === df_group_test("PK"), "inner").select(podunion_df("PK"),df_group_test("curr_pod_journey").alias("pod_journey"))

			podjourney_df = pod_test.withColumn("pod_journey", concat_ws("|", pod_test("pod_journey"))).distinct
			val podjourney_dfPwrite = podjourney_df.withColumnRenamed("pod_journey","CT_COL")

			podjourney_dfPwrite.write.mode("overwrite").parquet(tempPath + "podjourney_df1")

			podjourney_df = sqlContext.read.parquet(tempPath + "podjourney_df1")

			println("podjourney_df written to parquet")
		}
		if(inbjourney_df== null && obsjourney_df!=null){
			println("IBS Null and OBS Not Null")
			val podunion_df = obsjourney_df

			println("After ibs and obs Journey union")

			val df_group_test = podunion_df.groupBy(podunion_df("PK")).agg(sort_array(collect_list(podunion_df("journey"))).alias("curr_pod_journey"))

			val pod_test = podunion_df.join(df_group_test, podunion_df("PK") === df_group_test("PK"), "inner").select(podunion_df("PK"),df_group_test("curr_pod_journey").alias("pod_journey"))

			podjourney_df = pod_test.withColumn("pod_journey", concat_ws("|", pod_test("pod_journey"))).distinct
			val podjourney_dfPwrite = podjourney_df.withColumnRenamed("pod_journey","CT_COL")

			podjourney_dfPwrite.write.mode("overwrite").parquet(tempPath + "podjourney_df2")

			podjourney_df = sqlContext.read.parquet(tempPath + "podjourney_df2")
			println("podjourney_df written to parquet")

		}

		if(awbmst_df== null){
		println("awbmst_df is null, creating empty dataframe")
		awbmst_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
	  }
	  if(inbound_df== null){
		println("inbound_df is null, creating empty dataframe")
		inbound_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
	  }
	  if(outbound_df== null){
		println("outbound_df is null, creating empty dataframe")
		outbound_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
	  }
	  if(shipclt_df== null){
		println("shipclt_df is null, creating empty dataframe")
		shipclt_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
	  }
	  if(addressdtl_df== null){
		println("addressdtl_df is null, creating empty dataframe")
		addressdtl_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
	  }
	  if(callpus_df== null){
		println("callpus_df is null, creating empty dataframe")
		callpus_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
	  }
	  if(mdpkp_df== null){
		println("mdpkp_df is null, creating empty dataframe")
		mdpkp_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
	  }
		if(podjourney_df == null){
			println("podjourney_df is null, creating empty dataframe")
			podjourney_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)

		}
	  if(callpusjourney_df == null){
		println("callpus_journey is null, creating empty dataframe")
		callpusjourney_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)

	  }
	  
	  if(awblink_df == null){
		println("awblink_df is null, creating empty dataframe")
		awblink_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)

	  }
	  if(sryaltinst_df == null){
		println("sryaltinst_df is null, creating empty dataframe")
		sryaltinst_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)

	  }
	  if(mdpod_df == null){
		println("mdpod_df is null, creating empty dataframe")
		mdpod_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)

	  }
		if(shpfltmst_df == null){
			println("shpfltmst_df is null, creating empty dataframe")
			shpfltmst_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)

		}

		if(shpfltmstjourney_df == null){
			println("shpfltmstjourney_df is null, creating empty dataframe")
			shpfltmstjourney_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)

		}

	  val df_map = Map("ABM" -> awbmst_df,"IBS" -> inbound_df,"OBS" -> outbound_df,"SLT" -> shipclt_df, "ADT" -> addressdtl_df,"CPS" -> callpus_df,"MDP" -> mdpkp_df,"CPJ" -> callpusjourney_df,"ALK" -> awblink_df ,"SRT" -> sryaltinst_df,"MOD" -> mdpod_df,"SFM" -> shpfltmst_df,"PDJ" -> podjourney_df,"LTS" -> callpus_ts,"SFJ" -> shpfltmstjourney_df)


	  hbaseConn.persistInHbase(sc, df_map, sqlContext);

		/*		Archive processed Streaming Parquet files for all tables
		* 		Added on 30th July 2019
		* */
		println("Started archival of processed streaming parquet files ")

		archiveLastProcessed("awbmstDF_record",BATCH_INTERMEDIATE,STREAMING_PATH,ARCHIVE_PATH)
		archiveLastProcessed("shipcltDF_record",BATCH_INTERMEDIATE,STREAMING_PATH,ARCHIVE_PATH)
		archiveLastProcessed("inboundDF_record",BATCH_INTERMEDIATE,STREAMING_PATH,ARCHIVE_PATH)
		archiveLastProcessed("outboundDF_record",BATCH_INTERMEDIATE,STREAMING_PATH,ARCHIVE_PATH)
		archiveLastProcessed("callpusDF_record",BATCH_INTERMEDIATE,STREAMING_PATH,ARCHIVE_PATH)
		archiveLastProcessed("shipfltmstDF_record",BATCH_INTERMEDIATE,STREAMING_PATH,ARCHIVE_PATH)
		archiveLastProcessed("awblinkDF_record",BATCH_INTERMEDIATE,STREAMING_PATH,ARCHIVE_PATH)
		archiveLastProcessed("sryaltinstDF_record",BATCH_INTERMEDIATE,STREAMING_PATH,ARCHIVE_PATH)
		archiveLastProcessed("mdpodDF_record",BATCH_INTERMEDIATE,STREAMING_PATH,ARCHIVE_PATH)
		archiveLastProcessed("mdpickupDF_record",BATCH_INTERMEDIATE,STREAMING_PATH,ARCHIVE_PATH)

		println("Completed archival of processed streaming parquet files ")

		fetchLatestBatch(BATCH_INTERMEDIATE +"shipcltDF_record",FINAL_PATH +"shipcltDF_record")
	  fetchLatestBatch(BATCH_INTERMEDIATE +"awbmstDF_record",FINAL_PATH +"awbmstDF_record")
	  fetchLatestBatch(BATCH_INTERMEDIATE +"inboundDF_record",FINAL_PATH +"inboundDF_record")
	  fetchLatestBatch(BATCH_INTERMEDIATE +"outboundDF_record",FINAL_PATH +"outboundDF_record")
	  fetchLatestBatch(BATCH_INTERMEDIATE +"callpusDF_record",FINAL_PATH +"callpusDF_record")
	  fetchLatestBatch(BATCH_INTERMEDIATE +"shipfltmstDF_record",FINAL_PATH +"shipfltmstDF_record")
	  fetchLatestBatch(BATCH_INTERMEDIATE +"awblinkDF_record",FINAL_PATH +"awblinkDF_record")
	  fetchLatestBatch(BATCH_INTERMEDIATE +"sryaltinstDF_record",FINAL_PATH +"sryaltinstDF_record")
	  fetchLatestBatch(BATCH_INTERMEDIATE +"mdpodDF_record",FINAL_PATH +"mdpodDF_record")
	  fetchLatestBatch(BATCH_INTERMEDIATE +"mdpickupDF_record",FINAL_PATH +"mdpickupDF_record")

	  removeHadoopPath(BATCH_INTERMEDIATE+"awbmstDF_record",fs,logger)
	  removeHadoopPath(BATCH_INTERMEDIATE+"shipcltDF_record",fs,logger)
	  removeHadoopPath(BATCH_INTERMEDIATE+"inboundDF_record",fs,logger)
	  removeHadoopPath(BATCH_INTERMEDIATE+"outboundDF_record",fs,logger)
	  removeHadoopPath(BATCH_INTERMEDIATE+"callpusDF_record",fs,logger)
	  removeHadoopPath(BATCH_INTERMEDIATE+"shipfltmstDF_record",fs,logger)
	  removeHadoopPath(BATCH_INTERMEDIATE+"awblinkDF_record",fs,logger)
	  removeHadoopPath(BATCH_INTERMEDIATE+"sryaltinstDF_record",fs,logger)
	  removeHadoopPath(BATCH_INTERMEDIATE+"mdpodDF_record",fs,logger)
	  removeHadoopPath(BATCH_INTERMEDIATE+"mdpickupDF_record",fs,logger)

	  
	  println("Completed Parquet to Hbase IL PL load")

	
	
  }

}