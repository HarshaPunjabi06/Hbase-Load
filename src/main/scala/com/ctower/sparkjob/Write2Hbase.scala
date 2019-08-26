package com.ctower.sparkjob

import com.biapps.ctower.hbase.HbaseConnection
import com.ctower.util.schema.SchemaCommonStreaming._
import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object Write2Hbase {

  val zkQuorum = "172.18.114.95:2181" //for PROD
  val conf = new SparkConf().setAppName("Ops_CT_StreamingWrite2Hbase").setMaster("yarn-cluster")
  conf.set("spark.dynamicAllocation.enabled", "false")
  conf.set("spark.driver.memory", "40g")
  conf.set("spark.executor.memory", "12g")
  conf.set("spark.executor.cores", "5")
  conf.set("spark.executor.instances", "25")
  conf.set("spark.speculation", "true")
  conf.set("spark.streaming.kafka.maxRatePerPartition", "139")
  conf.set("spark.yarn.maxAppAttempts","4")
  conf.set("spark.yarn.am.attemptFailuresValidityInterval","1h")
  conf.set("spark.yarn.max.executor.failures","{8 * num_executors}")
  conf.set("spark.task.maxFailures","8")
 conf.set("principal", "hbase/sinhqcbiprden002.bluedart.com@BLUEDART.COM")
  conf.set("keytab", "hbase.keytab")
  conf.set("hbase.security.authentication", "kerberos")

  val sc = new SparkContext(conf)
  val groupId = "console-consumer-10007"
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

//  val ssc = new StreamingContext(sc, Seconds(300))

  //def createContext(): StreamingContext = {
  def createContext(checkpointDirectory: String): StreamingContext = {
  println("Creating new context with checkpointing")

    val ssc = new StreamingContext(sc, Seconds(300))
    val tempPath = "/Data/ControlTower/StreamingHbase/tempPath/"

    ssc.checkpoint(checkpointDirectory)
	
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

    val HbaseSchema = StructType(
      List(
        StructField("PK", StringType),
        StructField("CT_COL", StringType)))

//    val sdf = new SimpleDateFormat(dateFmt)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "172.18.114.91:9092,172.18.114.98:9092,172.18.114.95:9092", "group.id" -> groupId, "auto.offset.reset" -> "largest", "zookeeper.connection.timeout.ms" -> "10000", "zookeeper.session.timeout.ms" -> "10000")

    try {

      val message = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("bdIncrementalAllLegs")).map(_._2)
      println("DStream object Message created")

      message.foreachRDD { record =>
        if (record.take(1).length != 0) {
          println("Number of records : " + record.count)
          val ctrTopicDF = sqlContext.read.json(record)
          val awbmstDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.AWBMST")
          val delsheetDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.DELSHEET")
          val shipfltmstDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.SHIPFLTMST")
          val awbaddressdetailsDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.AWBADDRESSDETAILS")
          val locTrackerDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.LOCATIONTRACKER")
          val inboundStatusDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.INBOUNDSTATUSREMARK")
          val outboundStatusDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.OUTBOUNDSTATUSREMARK")
          val shipcltDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.SHIPCLT")
          val callpusDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.CALLPUS")
          val awboperationDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.AWBOPERATION")
          val mdpickupDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.MDPICKUP")
		  val awblinkDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.AWBLINK")
		  val sryaltinstDF = ctrTopicDF.filter(ctrTopicDF("table") === "BDMASTERLIVE.SRYALTINST")

          awbmst_df = null
          inbound_df = null
          outbound_df = null
          shipclt_df = null
          addressdtl_df = null
          mdpkp_df = null
          callpus_df = null
          inbjourney_df = null
          obsjourney_df = null
          podjourney_df = null
          callpusjourney_df = null


          if (!awbmstDF.rdd.isEmpty) {
            println("Inside AWBMST")
            val awbmstSchema = getAwbmstSchema(sc)
            val ctrTopicDF = sqlContext.read.schema(awbmstSchema).json(record)

            val awbmstDF_record = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.AWBMST").select("after.nawbid", "after.cawbno", "after.cprodcode", "after.cprodtype", "after.corgarea", "after.cdstarea", "after.cbatchno", "after.npcs", "after.dshipdate", "after.dpudate", "after.ntokenno", "after.cpuwi", "after.ndimweight", "after.ccustcode", "after.ncshmemno", "after.ccmdtycode", "after.ncolamt", "after.namt", "after.ctrncode", "after.csplinst", "after.cpacktype", "after.coda", "after.cmode", "after.cgsacode", "after.cfodcodflg", "after.cfoccode", "after.cbtpcode", "after.cbtparea", "after.cbillno", "after.brefno", "after.bduty", "after.csectyck", "after.nparrentcontid", "after.bbillcnee", "after.cpuempcode", "after.bstamdart", "after.corgscrcd", "after.cdstscrcd", "after.bpriority", "after.csubcode", "after.ncurlookupid", "after.cputime", "after.cflfm", "after.cdhlacctno", "after.crtoarea", "after.crtocode", "after.cemailid", "after.nweight", "after.noctroi", "after.nduty", "after.nsmartboxtypea", "after.nsmartboxtypeb", "after.ndccount", "after.cbillcac", "after.bcrcrdref", "after.ndcrecd", "after.cdlvpurtcd", "after.caccuracy", "after.cdlcode", "after.ckgpound", "after.cmloccode", "after.cadecode", "after.dinscandt", "after.cinscanloc", "after.bedptrnx", "after.bcrcrdpay", "after.biatadtl", "after.cdocattach", "after.cemplcode", "after.nopcs", "after.cmustgocrg", "after.codocattac", "after.cdataentryloc", "after.bshpcrcrdref", "after.deptdtdlv", "after.bdetain", "after.cisoverage", "after.cdhlflag", "after.ddemudt", "after.ndemuamt", "after.cdemucode", "after.cdemuloccode", "after.dstatusdt", "after.bcsbprinted", "after.ddataentrydt", "after.dbatchdt", "after.castattype", "after.castatcode", "after.cstatemplcode", "after.caremarks", "after.captcode", "after.bpwpalletized", "after.cprintmode", "after.cpromocode", "after.crtoimtly", "after.bdgshipment", "after.ispwrec", "after.cpreftm", "after.crevpu", "after.cpscode", "after.cfrdawbno", "after.crfdcompnm", "after.crefno2", "after.crefno3", "after.cpumode", "after.cputype", "after.nitemcnt", "after.bpartialpu", "after.npaycash", "after.nputmslot", "after.cmanifstno", "after.coffcltime", "after.ndeferreddeliverydays", "after.dcustedd", "after.cisddn", "after.cactdelloc", "after.cgstno", "after.cvehicleno", "after.cexchawb", "after.dprefdate", "after.cpreftime", "after.ndistance", "after.dcustpudt", "after.ccustputm", "after.cavailtime", "after.cavaildays", "after.cotptype", "after.notpno", "after.cpackagingid", "after.cccode", "op_ts", "op_type").withColumn("load_ts", expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))

			
			val awbmst_df1 = awbmstDF_record.orderBy("cawbno", "op_ts").registerTempTable("AWB_MST");

        awbmst_df = sqlContext.sql("""SELECT NVL(TRIM(cawbno),'NA!@#') AS PK, CONCAT_WS('~', NVL(TRIM(cawbno),'NA!@#'), NVL(nawbid,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(cprodtype,'NA!@#'), NVL(corgarea,'NA!@#'), NVL(cdstarea,'NA!@#'), NVL(cbatchno,'NA!@#'), NVL(npcs,'NA!@#'), NVL(dshipdate,'NA!@#'), NVL(dpudate,'NA!@#'), NVL(ntokenno,'NA!@#'), NVL(cpuwi,'NA!@#'), NVL(ndimweight,'NA!@#'), NVL(ccustcode,'NA!@#'), NVL(ncshmemno,'NA!@#'), NVL(ccmdtycode,'NA!@#'), NVL(ncolamt,'NA!@#'), NVL(namt,'NA!@#'), NVL(ctrncode,'NA!@#'), NVL(csplinst,'NA!@#'), NVL(cpacktype,'NA!@#'), NVL(coda,'NA!@#'), NVL(cmode,'NA!@#'), NVL(cgsacode,'NA!@#'), NVL(cfodcodflg,'NA!@#'), NVL(cfoccode,'NA!@#'), NVL(cbtpcode,'NA!@#'), NVL(cbtparea,'NA!@#'), NVL(cbillno,'NA!@#'), NVL(brefno,'NA!@#'), NVL(bduty,'NA!@#'), NVL(csectyck,'NA!@#'), NVL(nparrentcontid,'NA!@#'), NVL(bbillcnee,'NA!@#'), NVL(cpuempcode,'NA!@#'), NVL(bstamdart,'NA!@#'), NVL(corgscrcd,'NA!@#'), NVL(cdstscrcd,'NA!@#'), NVL(bpriority,'NA!@#'), NVL(csubcode,'NA!@#'), NVL(ncurlookupid,'NA!@#'), NVL(cputime,'NA!@#'), NVL(cflfm,'NA!@#'), NVL(cdhlacctno,'NA!@#'), NVL(crtoarea,'NA!@#'), NVL(crtocode,'NA!@#'), NVL(cemailid,'NA!@#'), NVL(nweight,'NA!@#'), NVL(noctroi,'NA!@#'), NVL(nduty,'NA!@#'), NVL(nsmartboxtypea,'NA!@#'), NVL(nsmartboxtypeb,'NA!@#'), NVL(ndccount,'NA!@#'), NVL(cbillcac,'NA!@#'), NVL(bcrcrdref,'NA!@#'), NVL(ndcrecd,'NA!@#'), NVL(cdlvpurtcd,'NA!@#'), NVL(caccuracy,'NA!@#'), NVL(cdlcode,'NA!@#'), NVL(ckgpound,'NA!@#'), NVL(cmloccode,'NA!@#'), NVL(cadecode,'NA!@#'), NVL(dinscandt,'NA!@#'), NVL(cinscanloc,'NA!@#'), NVL(bedptrnx,'NA!@#'), NVL(bcrcrdpay,'NA!@#'), NVL(biatadtl,'NA!@#'), NVL(cdocattach,'NA!@#'), NVL(cemplcode,'NA!@#'), NVL(nopcs,'NA!@#'), NVL(cmustgocrg,'NA!@#'), NVL(codocattac,'NA!@#'), NVL(cdataentryloc,'NA!@#'), NVL(bshpcrcrdref,'NA!@#'), NVL(deptdtdlv,'NA!@#'), NVL(bdetain,'NA!@#'), NVL(cisoverage,'NA!@#'), NVL(cdhlflag,'NA!@#'), NVL(ddemudt,'NA!@#'), NVL(ndemuamt,'NA!@#'), NVL(cdemucode,'NA!@#'), NVL(cdemuloccode,'NA!@#'), NVL(dstatusdt,'NA!@#'), NVL(bcsbprinted,'NA!@#'), NVL(ddataentrydt,'NA!@#'), NVL(dbatchdt,'NA!@#'), NVL(castattype,'NA!@#'), NVL(castatcode,'NA!@#'), NVL(cstatemplcode,'NA!@#'), NVL(caremarks,'NA!@#'), NVL(captcode,'NA!@#'), NVL(bpwpalletized,'NA!@#'), NVL(cprintmode,'NA!@#'), NVL(cpromocode,'NA!@#'), NVL(crtoimtly,'NA!@#'), NVL(bdgshipment,'NA!@#'), NVL(ispwrec,'NA!@#'), NVL(cpreftm,'NA!@#'), NVL(crevpu,'NA!@#'), NVL(cpscode,'NA!@#'), NVL(cfrdawbno,'NA!@#'), NVL(crfdcompnm,'NA!@#'), NVL(crefno2,'NA!@#'), NVL(crefno3,'NA!@#'), NVL(cpumode,'NA!@#'), NVL(cputype,'NA!@#'), NVL(nitemcnt,'NA!@#'), NVL(bpartialpu,'NA!@#'), NVL(npaycash,'NA!@#'), NVL(nputmslot,'NA!@#'), NVL(cmanifstno,'NA!@#'), NVL(coffcltime,'NA!@#'), NVL(ndeferreddeliverydays,'NA!@#'), NVL(dcustedd,'NA!@#'), NVL(cisddn,'NA!@#'), NVL(cactdelloc,'NA!@#'), NVL(cgstno,'NA!@#'), NVL(cvehicleno,'NA!@#'), NVL(cexchawb,'NA!@#'), NVL(dprefdate,'NA!@#'), NVL(cpreftime,'NA!@#'), NVL(ndistance,'NA!@#'), NVL(dcustpudt,'NA!@#'), NVL(ccustputm,'NA!@#'), NVL(cavailtime,'NA!@#'), NVL(cavaildays,'NA!@#'), NVL(cotptype,'NA!@#'), NVL(notpno,'NA!@#'), NVL(cpackagingid,'NA!@#'), NVL(cccode,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#')) AS CT_COL  from AWB_MST""")

            /*awbmst_dfPwrite.write.mode("overwrite").parquet(tempPath + "awbmst")

            awbmst_df = sqlContext.read.parquet(tempPath + "awbmst")

            println("awbmst written to parquet")*/

          } 
		  /*else {

            println("awbmst_df is null, creating empty dataframe")
            awbmst_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
          }*/

          /*if (!delsheetDF.rdd.isEmpty) {
			  println("Inside delsheet")
			val delsheetSchema = getDelsheetSchema(sc)
            val ctrTopicDF = sqlContext.read.schema(delsheetSchema).json(record)

            val delsheetDF_record = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.DELSHEET").select("after.ddate", "after.cemplcode", "after.nsrno", "after.cprodcode", "after.cawbno", "after.cmpsno", "after.ddeliverydt", "after.cremarks", "after.crelatcode", "after.crecievedby", "after.cidtype", "after.cidnumber", "after.cloccode", "after.dpodedate", "after.cpdecode", "after.bedptrnx", "after.cstattype", "after.cstatcode", "after.cstattime", "after.cvehicleno", "after.cdlvpurtcd", "after.cintllocation", "after.bprinted", "after.crelation", "after.cbatchno", "after.nroundno", "after.cprevawbno", "after.cunregvehicleno", "after.ccode", "after.cpscode", "after.cebnnolastmile", "op_ts","op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))

			delsheetDF_record.write.mode("append").partitionBy("load_ts").parquet(PATH1+"delsheetDF_record")

			println("DELSHEET records were written to ES")
          }

          if (!shipfltmstDF.rdd.isEmpty) {
			  println("Inside shipfltmst")
			val shipfltmstSchema = getShipfltmstSchema(sc)
            val ctrTopicDF = sqlContext.read.schema(shipfltmstSchema).json(record)

            val shipfltmstDF_record = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.SHIPFLTMST").select("after.cawbno", "after.cmpsno", "after.nrunid", "after.drundate", "after.cruncode", "after.corgarea", "after.cstattype", "after.cstatcode", "after.cemplcode", "after.cloccode", "after.nparrentcontid", "after.ddtinscan", "after.ctally", "after.bprinted", "after.dstatdate", "after.isundelshp", "after.isundeliverdate", "after.isnotoutscan", "after.isnotoutscandate", "after.crecdata", "after.doutscandt", "after.boutscan", "after.bedptrnx", "after.cdstarea", "after.cptally", "after.nrpcs", "after.cdocattach", "after.nopcs", "after.cflightno", "after.dflightdt", "after.dloadarrdate", "after.dfltarrdate", "after.cvehicleno", "after.nendkms", "after.dptallydate", "after.cactflightno", "after.dactflightdt", "after.ctallysrc", "after.ddstarrdt", "after.cdlvpurtcd", "after.ctallypurtcd", "after.nactparrentcontid", "after.cstatemplcode", "after.cstatclraction", "after.cstatclremplcode", "after.dstatclrdate", "after.cmstatcode", "op_ts","op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))

			shipfltmstDF_record.write.mode("append").partitionBy("load_ts").parquet(PATH1+"shipfltmstDF_record")
            println("SHIPFLTMST records were written to ES")
          }
*/
          if (!awbaddressdetailsDF.rdd.isEmpty) {
			  println("Inside AWBaddressdetails")
			     val awbaddressdetailsSchema = getAwbaddressdetailsSchema(sc)
            val ctrTopicDF = sqlContext.read.schema(awbaddressdetailsSchema).json(record)

            val awbaddressdetailsDF_record = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.AWBADDRESSDETAILS").select("after.nawbid", "after.cname", "after.ccode", "after.cattn", "after.cpincode", "after.cphoneno", "after.cmobile", "after.cemailid", "after.cfax", "after.cshipper", "after.naddpreference", "after.caddress1", "after.caddress2", "after.caddress3", "after.ccntrycode", "after.cstatecode", "after.cgstno", "after.cgpslat", "after.cgpslon", "after.cdsource", "after.cconmobmask", "after.caddresstype", "after.ccstmobmas", "after.crtomobmas", "op_ts","op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))

            //println(" adt count w/o join" + awbaddressdetailsDF_record.count())

            awbaddressdetailsDF_record.registerTempTable("adttbl")

            val awbaddressdetailsSelfJoin = sqlContext.sql("""  select bdw.op_ts,bdw.nawbid as nawbid, bdw.ccode as ccode, bdw.cattn as csender, bdwn.cattn as cattention from (Select nawbid, cshipper,ccode,op_ts,cattn from (select nawbid, cshipper,ccode,op_ts,cattn, row_number() over(partition by nawbid order by op_ts desc) as rnk from adttbl where cshipper = 'Y') as a where rnk = 1) as bdw join (Select nawbid, cshipper,ccode,op_ts,cattn from (select nawbid, cshipper,ccode,op_ts,cattn, row_number() over(partition by nawbid order by op_ts desc) as rnk from adttbl where cshipper = 'N') as b where rnk = 1) as bdwn
ON bdw.nawbid = bdwn.nawbid """)

            //println(" adt count w join" + awbaddressdetailsSelfJoin.count())

            //println("schema :::  " + awbaddressdetailsSelfJoin.printSchema())


            val adt_df1 = awbaddressdetailsSelfJoin.orderBy("nawbid", "op_ts").registerTempTable("ADT");

            /*addressdtl_df = sqlContext.sql("""SELECT NVL(nawbid,'NA!@#') AS PK, CONCAT_WS('~', NVL(nawbid,'NA!@#'), NVL(cname,'NA!@#'), NVL(ccode,'NA!@#'), NVL(cattn,'NA!@#'), NVL(cpincode,'NA!@#'), NVL(cphoneno,'NA!@#'), NVL(cmobile,'NA!@#'), NVL(cemailid,'NA!@#'), NVL(cfax,'NA!@#'), NVL(cshipper,'NA!@#'), NVL(naddpreference,'NA!@#'), NVL(caddress1,'NA!@#'), NVL(caddress2,'NA!@#'), NVL(caddress3,'NA!@#'), NVL(ccntrycode,'NA!@#'), NVL(cstatecode,'NA!@#'), NVL(cgstno,'NA!@#'), NVL(cgpslat,'NA!@#'), NVL(cgpslon,'NA!@#'), NVL(cdsource,'NA!@#'), NVL(cconmobmask,'NA!@#'), NVL(caddresstype,'NA!@#'), NVL(ccstmobmas,'NA!@#'), NVL(crtomobmas,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#')) AS CT_COL from ADT""")*/

            addressdtl_df = sqlContext.sql("""SELECT NVL(nawbid,'NA!@#') AS PK, CONCAT_WS('~', NVL(nawbid,'NA!@#'), NVL(ccode,'NA!@#'), NVL(cattention,'NA!@#'), NVL(csender,'NA!@#'), NVL(op_ts,'NA!@#')) AS CT_COL from ADT""")

            /*addressdtl_df_Pwrite.write.mode("overwrite").parquet(tempPath + "addressdetails")

            addressdtl_df = sqlContext.read.parquet(tempPath + "addressdetails")

            println("adddressdetails written to parquet")*/

          } 
		  /*else {

            println("addressdtl_df is null, creating empty dataframe")
            addressdtl_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
          }*/

          // Fetch Location Tracker Records

          /*if (!locTrackerDF.rdd.isEmpty) {
            //throw new Exception("Some Error Occured")
			println("Inside loctrack")
            val location_tracker_record = locTrackerDF.select("after.ntrackerid", "after.cawbno", "after.cmpsno", "after.nccid", "after.cloccode", "after.dindate", "after.binboundflag", "after.boutboundflag", "after.dfilename", "after.sfilename", "after.dfiledttm", "after.sfiledttm", "after.sedptrnx", "after.dedptrnx", "after.cawbtrnx", "after.cconflttrnx", "after.cuserapproval", "after.dapprovaldttm", "after.cedpname", "after.cdfileflag", "after.ddfiledate",  "op_ts","op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))

            location_tracker_record.write.mode("append").partitionBy("load_ts").parquet(PATH1+"locationtrackerDF_record")
            println("LocationTracker Records were written to parquet file")
          }*/
          //Fetch InboundStatusRemark records

          if (!inboundStatusDF.rdd.isEmpty) {
			  println("Inside inbound")

            val inboundSchema = getInboundSchema(sc)
            val ctrTopicDF = sqlContext.read.schema(inboundSchema).json(record)

            val inbound_record = inboundStatusDF.select("after.nstatusid", "after.noperationid", "after.cawbno", "after.cmpsno", "after.cstatcode", "after.cstattype", "after.dstatdate", "after.cremarks", "after.cloccode", "after.cemplcode", "after.cfilename", "after.sedptrnx", "after.dedptrnx", "after.dfilename", "after.cprodcode", "after.corgarea", "after.cdstarea", "after.dentdate", "after.dlastmodifiedts", "op_ts","op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))

            inbound_record.registerTempTable("ibsstream")

            inbjourney_df = sqlContext.sql("""select distinct cawbno AS PK, CONCAT(concat_ws('~',dstatdate,cstattype, cstatcode, cloccode, cemplcode),"@IB") as journey from ibsstream""")

			
			val inbound_df1 = inbound_record.orderBy("cawbno", "op_ts").registerTempTable("INB");
            
			inbound_df = sqlContext.sql(""" SELECT NVL(TRIM(cawbno),'NA!@#') AS PK, CONCAT_WS('~', NVL(TRIM(cawbno),'NA!@#'),  NVL(nstatusid,'NA!@#'), NVL(noperationid,'NA!@#'), NVL(cmpsno,'NA!@#'), NVL(cstatcode,'NA!@#'), NVL(cstattype,'NA!@#'), NVL(dstatdate,'NA!@#'), NVL(cremarks,'NA!@#'), NVL(cloccode,'NA!@#'), NVL(cemplcode,'NA!@#'), NVL(cfilename,'NA!@#'), NVL(sedptrnx,'NA!@#'), NVL(dedptrnx,'NA!@#'), NVL(dfilename,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(corgarea,'NA!@#'), NVL(cdstarea,'NA!@#'), NVL(dentdate,'NA!@#'), NVL(dlastmodifiedts,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#'))  AS CT_COL from INB """)

            /*inbound_dfPWrite.write.mode("overwrite").parquet(tempPath + "inbound_df")

            inbound_df = sqlContext.read.parquet(tempPath + "inbound_df")

            println("inbound written to parquet")*/

			
         } 
		  /*else {

            println("inbound_df is null, creating empty dataframe")
            inbound_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
          }*/
          //Fetch OutboundStatusRemark records

          if (!outboundStatusDF.rdd.isEmpty) {
			  println("Inside outbound")

            val outboundSchema = getOutboundSchema(sc)
            val ctrTopicDF = sqlContext.read.schema(outboundSchema).json(record)

            val outbound_record = outboundStatusDF.select("after.nstatusid", "after.noperationid", "after.cprodcode", "after.corgarea", "after.cdstarea", "after.cawbno", "after.cmpsno", "after.cstatcode", "after.cstattype", "after.dstatdate", "after.cremarks", "after.cloccode", "after.cemplcode", "after.cfilename", "after.sedptrnx", "after.dedptrnx", "after.ccno", "after.drundate", "after.dlastmodifiedts", "op_ts","op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))

            outbound_record.registerTempTable("obsstream")

            obsjourney_df = sqlContext.sql("""select distinct cawbno AS PK, CONCAT(concat_ws('~',dstatdate,cstattype, cstatcode, cloccode, cemplcode),"@OB") as journey from obsstream""")

			val outbound_df1 = outbound_record.orderBy("cawbno", "op_ts").registerTempTable("ONB");
            
			outbound_df = sqlContext.sql(""" SELECT NVL(TRIM(cawbno),'NA!@#') AS PK, CONCAT_WS('~', NVL(TRIM(cawbno),'NA!@#'),  NVL(nstatusid,'NA!@#'), NVL(noperationid,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(corgarea,'NA!@#'), NVL(cdstarea,'NA!@#'),  NVL(cmpsno,'NA!@#'), NVL(cstatcode,'NA!@#'), NVL(cstattype,'NA!@#'), NVL(dstatdate,'NA!@#'), NVL(cremarks,'NA!@#'), NVL(cloccode,'NA!@#'), NVL(cemplcode,'NA!@#'), NVL(cfilename,'NA!@#'), NVL(sedptrnx,'NA!@#'), NVL(dedptrnx,'NA!@#'), NVL(ccno,'NA!@#'), NVL(drundate,'NA!@#'), NVL(dlastmodifiedts,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#')) AS CT_COL from ONB""")

            /*outbound_dfPWrite.write.mode("overwrite").parquet(tempPath + "outbound_df")

            outbound_df = sqlContext.read.parquet(tempPath + "outbound_df")

            println("outbound written to parquet")*/


          } 
		  /*else {

            println("outbound_df is null, creating empty dataframe")
            outbound_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
          }*/

          if (!shipcltDF.rdd.isEmpty) {
			  println("Inside shipclt")
			    val shipclt_schema = getShipcltSchema(sc)
            val ctrTopicDF = sqlContext.read.schema(shipclt_schema).json(record)

            val shipclt_record = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.SHIPCLT").filter(ctrTopicDF("op_type") !== "D").select("after.cawbno", "after.cbatchno", "after.cbatorg", "after.cprodcode", "after.corgarea", "after.corgscrcd", "after.cmawbno", "after.cmloccode", "after.cdstarea", "after.cdstscrcd", "after.cmode", "after.cprodtype", "after.ctrncode", "after.cfoccode", "after.bpriority", "after.ccustcode", "after.csender", "after.ccneecode", "after.cattention", "after.npcs", "after.nweight", "after.nactwgt", "after.ccmdtycode", "after.ntokenno", "after.ncshmemno", "after.namt", "after.cbillno", "after.bbillcnee", "after.cbillcac", "after.cpacktype", "after.dshipdate", "after.dpudate", "after.cputime", "after.cpuemplcd", "after.cpuwi", "after.deptdtdlv", "after.coda", "after.cflightno", "after.dflightdt", "after.ckgpound", "after.blinked", "after.cadecode", "after.ccrcrdref", "after.ndiml", "after.ndimb", "after.ndimh", "after.nslabwgt", "after.nassdvalue", "after.cdocno", "after.ddocdate", "after.cpaytype", "after.namount", "after.cinvno", "after.dinvdate", "after.nothchrgs", "after.ncdutypc", "after.ccrcardno", "after.ccardcode", "after.ccardhold", "after.dvalidupto", "after.ccustname", "after.ccustadr1", "after.ccustadr2", "after.ccustadr3", "after.ccustpin", "after.ccusttel", "after.ccustfax", "after.ccneename", "after.ccneeadr1", "after.ccneeadr2", "after.ccneeadr3", "after.ccneepin", "after.ccneetel", "after.ccneefax", "after.bchecklst", "after.csplinst", "after.cproddesc", "after.dbatchdt", "after.noctroi", "after.cclectype", "after.ndclrdval", "after.bstampdart", "after.ccmdtydesc", "after.ccaltname", "after.caltattn", "after.ccaltadr1", "after.ccaltadr2", "after.ccaltadr3", "after.ccaltpin", "after.ccalttel", "after.ccaltfax", "after.ccneemob", "after.ccaltmob", "after.ncolamt", "after.cfodcodflg", "after.csubcode", "after.cctmno", "after.bdoxatchd", "after.cmrksnos1", "after.cmrksnos2", "after.cmrksnos3", "after.cdimen1", "after.cdimen2", "after.cdimen3", "after.nchrgwt", "after.ncomval", "after.nfreight", "after.nvalchgs", "after.nawbfee", "after.cawbfee", "after.nstatchg", "after.cstatchg", "after.ncartchg", "after.ccartchg", "after.nregtchg", "after.cregtchg", "after.nmiscchg1", "after.cmiscchg1", "after.nmiscchg2", "after.cmiscchg2", "after.nchgcolct", "after.coctrcptno", "after.cemailid", "after.cbtparea", "after.cbtpcode", "after.ccodfavour", "after.ccodpayble", "after.cfovtype", "after.naddisrchg", "after.naddosrchg", "after.ndocchrg", "after.ndcchrg", "after.nfodchrg", "after.nriskhchgs", "after.nodachrg", "after.cgsacode", "after.nfsamt", "after.cdhlflag", "after.ndodamt", "after.cloccode", "after.cilloc", "after.cdlcode", "after.ndiwt", "after.ccustmob", "after.cadnlcrcrdref", "after.dupldate", "after.cregcustname", "after.cregcustadr1", "after.cregcustadr2", "after.cregcustadr3", "after.cregcustpin", "after.cregcusttel", "after.crtoimdtly", "after.cpreftm", "after.crevpu", "after.cpscode", "after.ddwnlddate", "after.cfrdawbno", "after.crfdcompnm", "after.crefno2", "after.crefno3", "after.cpumode", "after.cputype", "after.nitemcnt", "after.bpartialpu", "after.npaycash", "after.nputmslot", "after.ccneemail", "after.crtocontnm", "after.crtoadr1", "after.crtoadr2", "after.crtoadr3", "after.crtopin", "after.crtotel", "after.crtomob", "after.cmanifstno", "after.crtolat", "after.crtolon", "after.crtoadrdt", "after.ccustlat", "after.ccustlon", "after.ccustadrdt", "after.ccneelat", "after.ccneelon", "after.ccneeadrdt", "after.ccaltlat", "after.ccaltlon", "after.ccaltadrdt", "after.cregcustlat", "after.cregcustlon", "after.cregcustadrdt", "after.coffcltime", "after.ndeferreddeliverydays", "after.cflfm", "after.ccntrycode", "after.cstatecode", "after.dcustedd", "after.cisddn", "after.cactdelloc", "after.cgstno", "after.ccustgstno", "after.ccneegstno", "after.cexchawb", "after.ccneefadd", "after.dcustpudt", "after.ccustputm", "after.cconmobmask", "after.caddresstype", "after.cavailtime", "after.cavaildays", "after.ccstmobmas", "after.crtomobmas", "after.cotptype", "after.notpno", "after.cincoterms", "after.cpurtcd", "after.cpuloc", "after.ccompgrp", "after.cpackagingid", "op_ts","op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')")).withColumn("op_flag", lit("I"))


			
			val shipclt_df1 = shipclt_record.orderBy("cawbno", "op_ts").registerTempTable("SLT");
            
			val shipclt_dfPwrite = sqlContext.sql(""" SELECT NVL(TRIM(cawbno),'NA!@#') AS PK, CONCAT_WS('~',NVL(TRIM(cawbno),'NA!@#'), NVL(cbatchno,'NA!@#'), NVL(cbatorg,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(corgarea,'NA!@#'), NVL(corgscrcd,'NA!@#'), NVL(cmawbno,'NA!@#'), NVL(cmloccode,'NA!@#'), NVL(cdstarea,'NA!@#'), NVL(cdstscrcd,'NA!@#'), NVL(cmode,'NA!@#'), NVL(cprodtype,'NA!@#'), NVL(ctrncode,'NA!@#'), NVL(cfoccode,'NA!@#'), NVL(bpriority,'NA!@#'), NVL(ccustcode,'NA!@#'), NVL(csender,'NA!@#'), NVL(ccneecode,'NA!@#'), NVL(cattention,'NA!@#'), NVL(npcs,'NA!@#'), NVL(nweight,'NA!@#'), NVL(nactwgt,'NA!@#'), NVL(ccmdtycode,'NA!@#'), NVL(ntokenno,'NA!@#'), NVL(ncshmemno,'NA!@#'), NVL(namt,'NA!@#'), NVL(cbillno,'NA!@#'), NVL(bbillcnee,'NA!@#'), NVL(cbillcac,'NA!@#'), NVL(cpacktype,'NA!@#'), NVL(dshipdate,'NA!@#'), NVL(dpudate,'NA!@#'), NVL(cputime,'NA!@#'), NVL(cpuemplcd,'NA!@#'), NVL(cpuwi,'NA!@#'), NVL(deptdtdlv,'NA!@#'), NVL(coda,'NA!@#'), NVL(cflightno,'NA!@#'), NVL(dflightdt,'NA!@#'), NVL(ckgpound,'NA!@#'), NVL(blinked,'NA!@#'), NVL(cadecode,'NA!@#'), NVL(ccrcrdref,'NA!@#'), NVL(ndiml,'NA!@#'), NVL(ndimb,'NA!@#'), NVL(ndimh,'NA!@#'), NVL(nslabwgt,'NA!@#'), NVL(nassdvalue,'NA!@#'), NVL(cdocno,'NA!@#'), NVL(ddocdate,'NA!@#'), NVL(cpaytype,'NA!@#'), NVL(namount,'NA!@#'), NVL(cinvno,'NA!@#'), NVL(dinvdate,'NA!@#'), NVL(nothchrgs,'NA!@#'), NVL(ncdutypc,'NA!@#'), NVL(ccrcardno,'NA!@#'), NVL(ccardcode,'NA!@#'), NVL(ccardhold,'NA!@#'), NVL(dvalidupto,'NA!@#'), NVL(ccustname,'NA!@#'), NVL(ccustadr1,'NA!@#'), NVL(ccustadr2,'NA!@#'), NVL(ccustadr3,'NA!@#'), NVL(ccustpin,'NA!@#'), NVL(ccusttel,'NA!@#'), NVL(ccustfax,'NA!@#'), NVL(ccneename,'NA!@#'), NVL(ccneeadr1,'NA!@#'), NVL(ccneeadr2,'NA!@#'), NVL(ccneeadr3,'NA!@#'), NVL(ccneepin,'NA!@#'), NVL(ccneetel,'NA!@#'), NVL(ccneefax,'NA!@#'), NVL(bchecklst,'NA!@#'), NVL(csplinst,'NA!@#'), NVL(cproddesc,'NA!@#'), NVL(dbatchdt,'NA!@#'), NVL(noctroi,'NA!@#'), NVL(cclectype,'NA!@#'), NVL(ndclrdval,'NA!@#'), NVL(bstampdart,'NA!@#'), NVL(ccmdtydesc,'NA!@#'), NVL(ccaltname,'NA!@#'), NVL(caltattn,'NA!@#'), NVL(ccaltadr1,'NA!@#'), NVL(ccaltadr2,'NA!@#'), NVL(ccaltadr3,'NA!@#'), NVL(ccaltpin,'NA!@#'), NVL(ccalttel,'NA!@#'), NVL(ccaltfax,'NA!@#'), NVL(ccneemob,'NA!@#'), NVL(ccaltmob,'NA!@#'), NVL(ncolamt,'NA!@#'), NVL(cfodcodflg,'NA!@#'), NVL(csubcode,'NA!@#'), NVL(cctmno,'NA!@#'), NVL(bdoxatchd,'NA!@#'), NVL(cmrksnos1,'NA!@#'), NVL(cmrksnos2,'NA!@#'), NVL(cmrksnos3,'NA!@#'), NVL(cdimen1,'NA!@#'), NVL(cdimen2,'NA!@#'), NVL(cdimen3,'NA!@#'), NVL(nchrgwt,'NA!@#'), NVL(ncomval,'NA!@#'), NVL(nfreight,'NA!@#'), NVL(nvalchgs,'NA!@#'), NVL(nawbfee,'NA!@#'), NVL(cawbfee,'NA!@#'), NVL(nstatchg,'NA!@#'), NVL(cstatchg,'NA!@#'), NVL(ncartchg,'NA!@#'), NVL(ccartchg,'NA!@#'), NVL(nregtchg,'NA!@#'), NVL(cregtchg,'NA!@#'), NVL(nmiscchg1,'NA!@#'), NVL(cmiscchg1,'NA!@#'), NVL(nmiscchg2,'NA!@#'), NVL(cmiscchg2,'NA!@#'), NVL(nchgcolct,'NA!@#'), NVL(coctrcptno,'NA!@#'), NVL(cemailid,'NA!@#'), NVL(cbtparea,'NA!@#'), NVL(cbtpcode,'NA!@#'), NVL(ccodfavour,'NA!@#'), NVL(ccodpayble,'NA!@#'), NVL(cfovtype,'NA!@#'), NVL(naddisrchg,'NA!@#'), NVL(naddosrchg,'NA!@#'), NVL(ndocchrg,'NA!@#'), NVL(ndcchrg,'NA!@#'), NVL(nfodchrg,'NA!@#'), NVL(nriskhchgs,'NA!@#'), NVL(nodachrg,'NA!@#'), NVL(cgsacode,'NA!@#'), NVL(nfsamt,'NA!@#'), NVL(cdhlflag,'NA!@#'), NVL(ndodamt,'NA!@#'), NVL(cloccode,'NA!@#'), NVL(cilloc,'NA!@#'), NVL(cdlcode,'NA!@#'), NVL(ndiwt,'NA!@#'), NVL(ccustmob,'NA!@#'), NVL(cadnlcrcrdref,'NA!@#'), NVL(dupldate,'NA!@#'), NVL(cregcustname,'NA!@#'), NVL(cregcustadr1,'NA!@#'), NVL(cregcustadr2,'NA!@#'), NVL(cregcustadr3,'NA!@#'), NVL(cregcustpin,'NA!@#'), NVL(cregcusttel,'NA!@#'), NVL(crtoimdtly,'NA!@#'), NVL(cpreftm,'NA!@#'), NVL(crevpu,'NA!@#'), NVL(cpscode,'NA!@#'), NVL(ddwnlddate,'NA!@#'), NVL(cfrdawbno,'NA!@#'), NVL(crfdcompnm,'NA!@#'), NVL(crefno2,'NA!@#'), NVL(crefno3,'NA!@#'), NVL(cpumode,'NA!@#'), NVL(cputype,'NA!@#'), NVL(nitemcnt,'NA!@#'), NVL(bpartialpu,'NA!@#'), NVL(npaycash,'NA!@#'), NVL(nputmslot,'NA!@#'), NVL(ccneemail,'NA!@#'), NVL(crtocontnm,'NA!@#'), NVL(crtoadr1,'NA!@#'), NVL(crtoadr2,'NA!@#'), NVL(crtoadr3,'NA!@#'), NVL(crtopin,'NA!@#'), NVL(crtotel,'NA!@#'), NVL(crtomob,'NA!@#'), NVL(cmanifstno,'NA!@#'), NVL(crtolat,'NA!@#'), NVL(crtolon,'NA!@#'), NVL(crtoadrdt,'NA!@#'), NVL(ccustlat,'NA!@#'), NVL(ccustlon,'NA!@#'), NVL(ccustadrdt,'NA!@#'), NVL(ccneelat,'NA!@#'), NVL(ccneelon,'NA!@#'), NVL(ccneeadrdt,'NA!@#'), NVL(ccaltlat,'NA!@#'), NVL(ccaltlon,'NA!@#'), NVL(ccaltadrdt,'NA!@#'), NVL(cregcustlat,'NA!@#'), NVL(cregcustlon,'NA!@#'), NVL(cregcustadrdt,'NA!@#'), NVL(coffcltime,'NA!@#'), NVL(ndeferreddeliverydays,'NA!@#'), NVL(cflfm,'NA!@#'), NVL(ccntrycode,'NA!@#'), NVL(cstatecode,'NA!@#'), NVL(dcustedd,'NA!@#'), NVL(cisddn,'NA!@#'), NVL(cactdelloc,'NA!@#'), NVL(cgstno,'NA!@#'), NVL(ccustgstno,'NA!@#'), NVL(ccneegstno,'NA!@#'), NVL(cexchawb,'NA!@#'), NVL(ccneefadd,'NA!@#'), NVL(dcustpudt,'NA!@#'), NVL(ccustputm,'NA!@#'), NVL(cconmobmask,'NA!@#'), NVL(caddresstype,'NA!@#'), NVL(cavailtime,'NA!@#'), NVL(cavaildays,'NA!@#'), NVL(ccstmobmas,'NA!@#'), NVL(crtomobmas,'NA!@#'), NVL(cotptype,'NA!@#'), NVL(notpno,'NA!@#'), NVL(cincoterms,'NA!@#'), NVL(cpurtcd,'NA!@#'), NVL(cpuloc,'NA!@#'), NVL(ccompgrp,'NA!@#'), NVL(cpackagingid,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#')) AS CT_COL from SLT """)

           shipclt_dfPwrite.write.mode("overwrite").parquet(tempPath + "shipclt")

            shipclt_df = sqlContext.read.parquet(tempPath + "shipclt")

            println("shipclt written to parquet")


          } /*else {

            println("shipclt_df is null, creating empty dataframe")
            shipclt_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
          }*/

         if (!callpusDF.rdd.isEmpty) {
            println("Inside callpus")
            val callpusSchema = getCallpusSchema(sc)

            val ctrTopicDF = sqlContext.read.schema(callpusSchema).json(record)

            val callpusDF_record = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.CALLPUS").select("after.carea", "after.cscrcd", "after.dregdate", "after.cprodcode", "after.cprodtype", "after.ctrncode", "after.ccustcode", "after.ccustname", "after.ccustadr1", "after.ccustadr2", "after.ccustadr3", "after.ccustpin", "after.ccusttel", "after.ccontact", "after.dpudate", "after.cputime", "after.cpupsdtm", "after.cremarks", "after.npcs", "after.nweight", "after.ntokenno", "after.cdlvpurtcd", "after.ctrnxpsd", "after.cpupsdemp", "after.bprinted", "after.ccustfax", "after.cmobileno", "after.cpsdbyemp", "after.cloggedby", "after.creminder", "after.dremdate", "after.coffcltime", "after.cfod", "after.ccod", "after.coda", "after.bpriority", "after.caltpurtcd", "after.cpuemplcd", "after.caltpuemp", "after.nsrno", "after.ctdd", "after.cmode", "after.dactpudate", "after.cactputime", "after.cactpuemp", "after.creschpu", "after.cemailid", "after.cbox", "after.cstatcode", "after.cstattype", "after.caltscrcd", "after.cregisterby", "after.cmodifiedby", "after.dmodifieddate", "after.nvolweight", "after.ctopay", "after.cpustatus", "after.islocked", "after.lockinguser", "after.cdtp", "after.csii", "after.cimpexp", "after.numberofbox", "after.creasonremark", "after.catmptime", "after.cawbno", "after.newntokenno", "after.dreassigneddate", "after.ccallername", "after.ccallerphone", "after.dreschdate", "after.bedptrnx", "after.cfilename", "after.cpuarea", "after.cisbulkupld", "after.cisdedone", "after.crefno", "after.etail", "after.crawbno", "after.dstatusentrydt", "after.crevpu", "after.nattempted", "after.cclstatcode", "after.cclstattype", "after.cclatmptime", "after.callstatus", "after.cisnewpu", "after.cisddn", "after.cgstno", "op_ts", "op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))

            callpusDF_record.registerTempTable("callpus")

            val cpjor = sqlContext.sql("""select distinct ntokenno AS PK, concat_ws('~',dregdate,dpudate,crevpu) as journey from callpus""")
            val df_group_test = cpjor.groupBy(cpjor("PK")).agg(sort_array(collect_list(cpjor("journey"))).alias("curr_cp_journey"))

            val cp_test = cpjor.join(df_group_test, cpjor("PK") === df_group_test("PK"), "inner").select(cpjor("PK"),df_group_test("curr_cp_journey").alias("cp_journey"))



            callpusjourney_df = cp_test.withColumn("cp_journey", concat_ws("|", cp_test("cp_journey"))).distinct
            callpusjourney_df = callpusjourney_df.withColumnRenamed("cp_journey","CT_COL")

            println("CallPUS journey:")
            callpusjourney_df.show(false)


            val callpus_df1 = callpusDF_record.orderBy("ntokenno", "op_ts").registerTempTable("cpsdf");

           val callpus_dfPWrite = sqlContext.sql(""" SELECT NVL(ntokenno,'NA!@#') as PK, CONCAT_WS('~',NVL(ntokenno,'NA!@#'), NVL(carea,'NA!@#'), NVL(cscrcd,'NA!@#'), NVL(dregdate,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(cprodtype,'NA!@#'), NVL(ctrncode,'NA!@#'), NVL(ccustcode,'NA!@#'), NVL(ccustname,'NA!@#'), NVL(ccustadr1,'NA!@#'), NVL(ccustadr2,'NA!@#'), NVL(ccustadr3,'NA!@#'), NVL(ccustpin,'NA!@#'), NVL(ccusttel,'NA!@#'), NVL(ccontact,'NA!@#'), NVL(dpudate,'NA!@#'), NVL(cputime,'NA!@#'), NVL(cpupsdtm,'NA!@#'), NVL(cremarks,'NA!@#'), NVL(npcs,'NA!@#'), NVL(nweight,'NA!@#'), NVL(cdlvpurtcd,'NA!@#'), NVL(ctrnxpsd,'NA!@#'), NVL(cpupsdemp,'NA!@#'), NVL(bprinted,'NA!@#'), NVL(ccustfax,'NA!@#'), NVL(cmobileno,'NA!@#'), NVL(cpsdbyemp,'NA!@#'), NVL(cloggedby,'NA!@#'), NVL(creminder,'NA!@#'), NVL(dremdate,'NA!@#'), NVL(coffcltime,'NA!@#'), NVL(cfod,'NA!@#'), NVL(ccod,'NA!@#'), NVL(coda,'NA!@#'), NVL(bpriority,'NA!@#'), NVL(caltpurtcd,'NA!@#'), NVL(cpuemplcd,'NA!@#'), NVL(caltpuemp,'NA!@#'), NVL(nsrno,'NA!@#'), NVL(ctdd,'NA!@#'), NVL(cmode,'NA!@#'), NVL(dactpudate,'NA!@#'), NVL(cactputime,'NA!@#'), NVL(cactpuemp,'NA!@#'), NVL(creschpu,'NA!@#'), NVL(cemailid,'NA!@#'), NVL(cbox,'NA!@#'), NVL(cstatcode,'NA!@#'), NVL(cstattype,'NA!@#'), NVL(caltscrcd,'NA!@#'), NVL(cregisterby,'NA!@#'), NVL(cmodifiedby,'NA!@#'), NVL(dmodifieddate,'NA!@#'), NVL(nvolweight,'NA!@#'), NVL(ctopay,'NA!@#'), NVL(cpustatus,'NA!@#'), NVL(islocked,'NA!@#'), NVL(lockinguser,'NA!@#'), NVL(cdtp,'NA!@#'), NVL(csii,'NA!@#'), NVL(cimpexp,'NA!@#'), NVL(numberofbox,'NA!@#'), NVL(creasonremark,'NA!@#'), NVL(catmptime,'NA!@#'), NVL(TRIM(cawbno),'NA!@#'), NVL(newntokenno,'NA!@#'), NVL(dreassigneddate,'NA!@#'), NVL(ccallername,'NA!@#'), NVL(ccallerphone,'NA!@#'), NVL(dreschdate,'NA!@#'), NVL(bedptrnx,'NA!@#'), NVL(cfilename,'NA!@#'), NVL(cpuarea,'NA!@#'), NVL(cisbulkupld,'NA!@#'), NVL(cisdedone,'NA!@#'), NVL(crefno,'NA!@#'), NVL(etail,'NA!@#'), NVL(crawbno,'NA!@#'), NVL(dstatusentrydt,'NA!@#'), NVL(crevpu,'NA!@#'), NVL(nattempted,'NA!@#'), NVL(cclstatcode,'NA!@#'), NVL(cclstattype,'NA!@#'), NVL(cclatmptime,'NA!@#'), NVL(callstatus,'NA!@#'), NVL(cisnewpu,'NA!@#'), NVL(cisddn,'NA!@#'), NVL(cgstno,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#')) as CT_COL from cpsdf""")

            callpus_dfPWrite.write.mode("overwrite").parquet(tempPath + "callpus")

            callpus_df = sqlContext.read.parquet(tempPath + "callpus")

            println("callpus written to parquet")

          } /*else {

            println("callpus_df is null, creating empty dataframe")
            callpus_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
          }*/
          if (!mdpickupDF.rdd.isEmpty) {

            println("Inside mdpickup")

            val mdpickupSchema = getmdpickupSchema(sc)
            val ctrTopicDF = sqlContext.read.schema(mdpickupSchema).json(record)

            val mdpickupDF_record = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.MDPICKUP").select("after.cpickupid", "after.cawbno", "after.cprodcode", "after.ccustcode", "after.corgarea", "after.corgscrcd", "after.cdstarea", "after.cdstscrcd", "after.csubcode", "after.cstatcode", "after.dstatusdate", "after.cmloccode", "after.cemplcode", "after.cdeviceno", "after.csimno", "after.cgpslat", "after.cgpslon", "after.cgpstime", "after.cgpssatcnt", "after.duplddt", "after.dsyncdate", "after.cstatus", "after.cpurtcode", "after.cispartialpickup", "after.creasonofpickuprejection", "after.cstatustype", "after.cispickupcancelled", "after.nputmslot", "after.ctype", "after.dnpudate", "after.cmdpickupdetid", "after.cremarks", "op_ts", "op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))

            val mdpkp_df1 = mdpickupDF_record.orderBy("cawbno", "op_ts").registerTempTable("mdpkp");

            mdpkp_df = sqlContext.sql(""" SELECT NVL(TRIM(cawbno),'NA!@#') AS PK ,CONCAT_WS('~', NVL(TRIM(cawbno),'NA!@#'),NVL(cpickupid,'NA!@#'), NVL(cprodcode,'NA!@#'), NVL(ccustcode,'NA!@#'), NVL(corgarea,'NA!@#'), NVL(corgscrcd,'NA!@#'), NVL(cdstarea,'NA!@#'), NVL(cdstscrcd,'NA!@#'), NVL(csubcode,'NA!@#'), NVL(cstatcode,'NA!@#'), NVL(dstatusdate,'NA!@#'), NVL(cmloccode,'NA!@#'), NVL(cemplcode,'NA!@#'), NVL(cdeviceno,'NA!@#'), NVL(csimno,'NA!@#'), NVL(cgpslat,'NA!@#'), NVL(cgpslon,'NA!@#'), NVL(cgpstime,'NA!@#'), NVL(cgpssatcnt,'NA!@#'), NVL(duplddt,'NA!@#'), NVL(dsyncdate,'NA!@#'), NVL(cstatus,'NA!@#'), NVL(cpurtcode,'NA!@#'), NVL(cispartialpickup,'NA!@#'), NVL(creasonofpickuprejection,'NA!@#'), NVL(cstatustype,'NA!@#'), NVL(cispickupcancelled,'NA!@#'), NVL(nputmslot,'NA!@#'), NVL(ctype,'NA!@#'), NVL(dnpudate,'NA!@#'), NVL(cmdpickupdetid,'NA!@#'), NVL(cremarks,'NA!@#'), NVL(op_ts,'NA!@#'), NVL(op_type,'NA!@#')) AS CT_COL from mdpkp""")

            /*mdpkp_dfPwrite.write.mode("overwrite").parquet(tempPath + "mdpickup")

            mdpkp_df = sqlContext.read.parquet(tempPath + "mdpickup")

            println("mdpickup written to parquet")*/
          } 
		  if (!awblinkDF.rdd.isEmpty) {

            println("Inside awblink")

            val awblinkSchema = getawblinkSchema(sc)
            val ctrTopicDF = sqlContext.read.schema(awblinkSchema).json(record)

			val awblinkDF_record = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.AWBLINK").select("after.coawbno", "after.coorgarea", "after.codstarea", "after.cnawbno", "after.cnorgarea", "after.cndstarea", "after.namt", "after.cprodcode", "after.ddate", "after.dflightdt", "after.cflag", "after.cprodtype", "after.cflightno", "after.cloccode", "after.ddfiledate", "op_ts", "op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))

            val awblink_df1 = awblinkDF_record.orderBy("coawbno", "op_ts").registerTempTable("awblink");

            awblink_df = sqlContext.sql("""SELECT NVL(COAWBNO,'NA!@#') AS PK, CONCAT_WS('~', NVL(COAWBNO,'NA!@#'), NVL(COORGAREA,'NA!@#'), NVL(CODSTAREA,'NA!@#'), NVL(CNAWBNO,'NA!@#'), NVL(CNORGAREA,'NA!@#'), NVL(CNDSTAREA,'NA!@#'), NVL(NAMT,'NA!@#'), NVL(CPRODCODE,'NA!@#'), NVL(DDATE,'NA!@#'), NVL(DFLIGHTDT,'NA!@#'), NVL(CFLAG,'NA!@#'), NVL(CPRODTYPE,'NA!@#'), NVL(CFLIGHTNO,'NA!@#'), NVL(CLOCCODE,'NA!@#'), NVL(DDFILEDATE,'NA!@#')) AS CT_COL from awblink""")

          }
		  if (!sryaltinstDF.rdd.isEmpty) {

            println("Inside sryaltinstDF")

            val sryaltinstSchema = getsryaltinstSchema(sc)
            val ctrTopicDF = sqlContext.read.schema(sryaltinstSchema).json(record)

            val sryaltinstDF_record = ctrTopicDF.filter(ctrTopicDF("table") === "BDMASTERLIVE.SRYALTINST").select("after.ctype", "after.cemplcode", "after.ctimearea", "after.dattempdt", "after.cemplemail", "after.cemplname", "after.cattention", "after.cawbno", "after.cnawbno", "after.dentdate", "after.centtime", "after.corgarea", "after.cdstarea", "after.cprodcode", "after.ccustcode", "after.cmode", "after.cmailloc", "after.cremarks", "after.ccustname", "after.ctyp", "after.bsryaltinst", "after.crecdby", "after.crelation", "after.cstatcode", "after.cpuwi", "after.dimportdt", "op_ts", "op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))
			
			sryaltinstDF_record.show(false)

            val sryaltinst_df1 = sryaltinstDF_record.orderBy("cawbno", "op_ts").registerTempTable("sryaltinst");

            sryaltinst_df = sqlContext.sql("""SELECT NVL(CAWBNO,'NA!@#') AS PK, CONCAT_WS('~',NVL(CAWBNO,'NA!@#'), NVL(CTYPE,'NA!@#'), NVL(CEMPLCODE,'NA!@#'), NVL(CTIMEAREA,'NA!@#'), NVL(DATTEMPDT,'NA!@#'), NVL(CEMPLEMAIL,'NA!@#'), NVL(CEMPLNAME,'NA!@#'), NVL(CATTENTION,'NA!@#'), NVL(CNAWBNO,'NA!@#'), NVL(DENTDATE,'NA!@#'), NVL(CENTTIME,'NA!@#'), NVL(CORGAREA,'NA!@#'), NVL(CDSTAREA,'NA!@#'), NVL(CPRODCODE,'NA!@#'), NVL(CCUSTCODE,'NA!@#'), NVL(CMODE,'NA!@#'), NVL(CMAILLOC,'NA!@#'), NVL(CREMARKS,'NA!@#'), NVL(CCUSTNAME,'NA!@#'), NVL(CTYP,'NA!@#'), NVL(BSRYALTINST,'NA!@#'), NVL(CRECDBY,'NA!@#'), NVL(CRELATION,'NA!@#'), NVL(CSTATCODE,'NA!@#'), NVL(CPUWI,'NA!@#'), NVL(DIMPORTDT,'NA!@#')) AS CT_COL from sryaltinst""")
			
			

            /*mdpkp_dfPwrite.write.mode("overwrite").parquet(tempPath + "mdpickup")

            mdpkp_df = sqlContext.read.parquet(tempPath + "mdpickup")

            println("mdpickup written to parquet")*/
          }
		  
		  
		  /*else {

            println("mdpkp_df is null, creating empty dataframe")
            mdpkp_df = sqlContext.createDataFrame(sc.emptyRDD[Row], HbaseSchema)
          }*/







		  /*if (!awboperationDF.rdd.isEmpty) {
            val awboperationSchema = getawboperationSchema(sc)

            val ctrTopicDF = sqlContext.read.schema(awboperationSchema).json(record)

            val awboperationDF_record = ctrTopicDF.filter(ctrTopicDF("table") === "BDDATA.AWBOPERATION").select("after.nawbopid", "after.noplookupid", "after.cloccode", "after.cawbno", "after.cemplcode", "after.doperationdate", "op_ts", "op_type").withColumn("load_ts",expr("from_unixtime(unix_timestamp(),'yyyyMMddHHmm')"))

            awboperationDF_record.write.mode("append").partitionBy("load_ts").parquet(PATH1+"awboperationDF_record")
            println("AWBOPERATION Records were written to parquet file")
          }

		 */

        }

        if (!record.isEmpty()) {

          if(inbjourney_df!= null && obsjourney_df!=null){
            println("IBS and OBS Not Null")
            val podunion_df = inbjourney_df.unionAll(obsjourney_df)
            podunion_df.show(false)

            println("After ibs and obs Journey union")

            val df_group_test = podunion_df.groupBy(podunion_df("PK")).agg(sort_array(collect_list(podunion_df("journey"))).alias("curr_pod_journey"))

            val pod_test = podunion_df.join(df_group_test, podunion_df("PK") === df_group_test("PK"), "inner").select(podunion_df("PK"),df_group_test("curr_pod_journey").alias("pod_journey"))

            podjourney_df = pod_test.withColumn("pod_journey", concat_ws("|", pod_test("pod_journey"))).distinct
            val podjourney_dfPwrite = podjourney_df.withColumnRenamed("pod_journey","CT_COL")

            podjourney_dfPwrite.write.mode("overwrite").parquet(tempPath + "podjourney_df")

            podjourney_df = sqlContext.read.parquet(tempPath + "podjourney_df")

            println("podjourney_df written to parquet")

            podjourney_df.show(false)
          }
          if(inbjourney_df!= null && obsjourney_df==null){
            println("IBS Not Null and OBS Null")
            val podunion_df = inbjourney_df
            podunion_df.show(false)

            println("After ibs and obs Journey union")

            val df_group_test = podunion_df.groupBy(podunion_df("PK")).agg(sort_array(collect_list(podunion_df("journey"))).alias("curr_pod_journey"))

            val pod_test = podunion_df.join(df_group_test, podunion_df("PK") === df_group_test("PK"), "inner").select(podunion_df("PK"),df_group_test("curr_pod_journey").alias("pod_journey"))

            podjourney_df = pod_test.withColumn("pod_journey", concat_ws("|", pod_test("pod_journey"))).distinct
            val podjourney_dfPwrite = podjourney_df.withColumnRenamed("pod_journey","CT_COL")

            podjourney_dfPwrite.write.mode("overwrite").parquet(tempPath + "podjourney_df1")

            podjourney_df = sqlContext.read.parquet(tempPath + "podjourney_df1")

            println("podjourney_df written to parquet")

            podjourney_df.show(false)
          }
          if(inbjourney_df== null && obsjourney_df!=null){
            println("IBS Null and OBS Not Null")
            val podunion_df = obsjourney_df
            podunion_df.show(false)

            println("After ibs and obs Journey union")

            val df_group_test = podunion_df.groupBy(podunion_df("PK")).agg(sort_array(collect_list(podunion_df("journey"))).alias("curr_pod_journey"))

            val pod_test = podunion_df.join(df_group_test, podunion_df("PK") === df_group_test("PK"), "inner").select(podunion_df("PK"),df_group_test("curr_pod_journey").alias("pod_journey"))

            podjourney_df = pod_test.withColumn("pod_journey", concat_ws("|", pod_test("pod_journey"))).distinct
            val podjourney_dfPwrite = podjourney_df.withColumnRenamed("pod_journey","CT_COL")

            podjourney_dfPwrite.write.mode("overwrite").parquet(tempPath + "podjourney_df2")

            podjourney_df = sqlContext.read.parquet(tempPath + "podjourney_df2")
            println("podjourney_df written to parquet")

            podjourney_df.show(false)
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
		  

          val df_map = Map("ABM" -> awbmst_df,"IBS" -> inbound_df,"OBS" -> outbound_df,"SLT" -> shipclt_df, "ADT" -> addressdtl_df,"CPS" -> callpus_df,"MDP" -> mdpkp_df,"CPJ" -> callpusjourney_df,"PDJ" -> podjourney_df,"ALK" -> awblink_df ,"SRT" -> sryaltinst_df)


          println("Starting HBASE: ")

          val hbaseConn = new HbaseConnection()
          hbaseConn.persistInHbase(sc, df_map, sqlContext);


        }


      }

    } catch {
      case ex: Throwable => {
        println("Found Exception :", ex.getMessage())
        ssc.stop()
        //System.exit(1)

      }
    }
    ssc
  }

  def main(args: Array[String]) {

    val checkpointDirectory : String = "/user/datalake/ajangid/cp/"

    val ssc = StreamingContext.getOrCreate(
      checkpointDirectory,
      () => createContext(checkpointDirectory))

//    createContext()
    ssc.start()
    ssc.awaitTermination()

  }
}