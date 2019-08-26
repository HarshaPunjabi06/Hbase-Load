package com.ctower.util.schema
import org.apache.spark.sql.types.{StringType,LongType}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.broadcast.Broadcast
import org.apache.spark._


object SchemaCommonStreaming {
  
  def getShipcltSchema(sc:SparkContext):StructType = {
      val shipcltDF = StructType(
    List(
	StructField("CAWBNO", StringType),
	StructField("CBATCHNO", StringType),
	StructField("CBATORG", StringType),
	StructField("CPRODCODE", StringType),
	StructField("CORGAREA", StringType),
	StructField("CORGSCRCD", StringType),
	StructField("CMAWBNO", StringType),
	StructField("CMLOCCODE", StringType),
	StructField("CDSTAREA", StringType),
	StructField("CDSTSCRCD", StringType),
	StructField("CMODE", StringType),
	StructField("CPRODTYPE", StringType),
	StructField("CTRNCODE", StringType),
	StructField("CFOCCODE", StringType),
	StructField("BPRIORITY", StringType),
	StructField("CCUSTCODE", StringType),
	StructField("CSENDER", StringType),
	StructField("CCNEECODE", StringType),
	StructField("CATTENTION", StringType),
	StructField("NPCS", StringType),
	StructField("NWEIGHT", StringType),
	StructField("NACTWGT", StringType),
	StructField("CCMDTYCODE", StringType),
	StructField("NTOKENNO", StringType),
	StructField("NCSHMEMNO", StringType),
	StructField("NAMT", StringType),
	StructField("CBILLNO", StringType),
	StructField("BBILLCNEE", StringType),
	StructField("CBILLCAC", StringType),
	StructField("CPACKTYPE", StringType),
	StructField("DSHIPDATE", StringType),
	StructField("DPUDATE", StringType),
	StructField("CPUTIME", StringType),
	StructField("CPUEMPLCD", StringType),
	StructField("CPUWI", StringType),
	StructField("DEPTDTDLV", StringType),
	StructField("CODA", StringType),
	StructField("CFLIGHTNO", StringType),
	StructField("DFLIGHTDT", StringType),
	StructField("CKGPOUND", StringType),
	StructField("BLINKED", StringType),
	StructField("CADECODE", StringType),
	StructField("CCRCRDREF", StringType),
	StructField("NDIML", StringType),
	StructField("NDIMB", StringType),
	StructField("NDIMH", StringType),
	StructField("NSLABWGT", StringType),
	StructField("NASSDVALUE", StringType),
	StructField("CDOCNO", StringType),
	StructField("DDOCDATE", StringType),
	StructField("CPAYTYPE", StringType),
	StructField("NAMOUNT", StringType),
	StructField("CINVNO", StringType),
	StructField("DINVDATE", StringType),
	StructField("NOTHCHRGS", StringType),
	StructField("NCDUTYPC", StringType),
	StructField("CCRCARDNO", StringType),
	StructField("CCARDCODE", StringType),
	StructField("CCARDHOLD", StringType),
	StructField("DVALIDUPTO", StringType),
	StructField("CCUSTNAME", StringType),
	StructField("CCUSTADR1", StringType),
	StructField("CCUSTADR2", StringType),
	StructField("CCUSTADR3", StringType),
	StructField("CCUSTPIN", StringType),
	StructField("CCUSTTEL", StringType),
	StructField("CCUSTFAX", StringType),
	StructField("CCNEENAME", StringType),
	StructField("CCNEEADR1", StringType),
	StructField("CCNEEADR2", StringType),
	StructField("CCNEEADR3", StringType),
	StructField("CCNEEPIN", StringType),
	StructField("CCNEETEL", StringType),
	StructField("CCNEEFAX", StringType),
	StructField("BCHECKLST", StringType),
	StructField("CSPLINST", StringType),
	StructField("CPRODDESC", StringType),
	StructField("DBATCHDT", StringType),
	StructField("NOCTROI", StringType),
	StructField("CCLECTYPE", StringType),
	StructField("NDCLRDVAL", StringType),
	StructField("BSTAMPDART", StringType),
	StructField("CCMDTYDESC", StringType),
	StructField("CCALTNAME", StringType),
	StructField("CALTATTN", StringType),
	StructField("CCALTADR1", StringType),
	StructField("CCALTADR2", StringType),
	StructField("CCALTADR3", StringType),
	StructField("CCALTPIN", StringType),
	StructField("CCALTTEL", StringType),
	StructField("CCALTFAX", StringType),
	StructField("CCNEEMOB", StringType),
	StructField("CCALTMOB", StringType),
	StructField("NCOLAMT", StringType),
	StructField("CFODCODFLG", StringType),
	StructField("CSUBCODE", StringType),
	StructField("CCTMNO", StringType),
	StructField("BDOXATCHD", StringType),
	StructField("CMRKSNOS1", StringType),
	StructField("CMRKSNOS2", StringType),
	StructField("CMRKSNOS3", StringType),
	StructField("CDIMEN1", StringType),
	StructField("CDIMEN2", StringType),
	StructField("CDIMEN3", StringType),
	StructField("NCHRGWT", StringType),
	StructField("NCOMVAL", StringType),
	StructField("NFREIGHT", StringType),
	StructField("NVALCHGS", StringType),
	StructField("NAWBFEE", StringType),
	StructField("CAWBFEE", StringType),
	StructField("NSTATCHG", StringType),
	StructField("CSTATCHG", StringType),
	StructField("NCARTCHG", StringType),
	StructField("CCARTCHG", StringType),
	StructField("NREGTCHG", StringType),
	StructField("CREGTCHG", StringType),
	StructField("NMISCCHG1", StringType),
	StructField("CMISCCHG1", StringType),
	StructField("NMISCCHG2", StringType),
	StructField("CMISCCHG2", StringType),
	StructField("NCHGCOLCT", StringType),
	StructField("COCTRCPTNO", StringType),
	StructField("CEMAILID", StringType),
	StructField("CBTPAREA", StringType),
	StructField("CBTPCODE", StringType),
	StructField("CCODFAVOUR", StringType),
	StructField("CCODPAYBLE", StringType),
	StructField("CFOVTYPE", StringType),
	StructField("NADDISRCHG", StringType),
	StructField("NADDOSRCHG", StringType),
	StructField("NDOCCHRG", StringType),
	StructField("NDCCHRG", StringType),
	StructField("NFODCHRG", StringType),
	StructField("NRISKHCHGS", StringType),
	StructField("NODACHRG", StringType),
	StructField("CGSACODE", StringType),
	StructField("NFSAMT", StringType),
	StructField("CDHLFLAG", StringType),
	StructField("NDODAMT", StringType),
	StructField("CLOCCODE", StringType),
	StructField("CILLOC", StringType),
	StructField("CDLCODE", StringType),
	StructField("NDIWT", StringType),
	StructField("CCUSTMOB", StringType),
	StructField("CADNLCRCRDREF", StringType),
	StructField("DUPLDATE", StringType),
	StructField("CREGCUSTNAME", StringType),
	StructField("CREGCUSTADR1", StringType),
	StructField("CREGCUSTADR2", StringType),
	StructField("CREGCUSTADR3", StringType),
	StructField("CREGCUSTPIN", StringType),
	StructField("CREGCUSTTEL", StringType),
	StructField("CRTOIMDTLY", StringType),
	StructField("CPREFTM", StringType),
	StructField("CREVPU", StringType),
	StructField("CPSCODE", StringType),
	StructField("DDWNLDDATE", StringType),
	StructField("CFRDAWBNO", StringType),
	StructField("CRFDCOMPNM", StringType),
	StructField("CREFNO2", StringType),
	StructField("CREFNO3", StringType),
	StructField("CPUMODE", StringType),
	StructField("CPUTYPE", StringType),
	StructField("NITEMCNT", StringType),
	StructField("BPARTIALPU", StringType),
	StructField("NPAYCASH", StringType),
	StructField("NPUTMSLOT", StringType),
	StructField("CCNEEMAIL", StringType),
	StructField("CRTOCONTNM", StringType),
	StructField("CRTOADR1", StringType),
	StructField("CRTOADR2", StringType),
	StructField("CRTOADR3", StringType),
	StructField("CRTOPIN", StringType),
	StructField("CRTOTEL", StringType),
	StructField("CRTOMOB", StringType),
	StructField("CMANIFSTNO", StringType),
	StructField("CRTOLAT", StringType),
	StructField("CRTOLON", StringType),
	StructField("CRTOADRDT", StringType),
	StructField("CCUSTLAT", StringType),
	StructField("CCUSTLON", StringType),
	StructField("CCUSTADRDT", StringType),
	StructField("CCNEELAT", StringType),
	StructField("CCNEELON", StringType),
	StructField("CCNEEADRDT", StringType),
	StructField("CCALTLAT", StringType),
	StructField("CCALTLON", StringType),
	StructField("CCALTADRDT", StringType),
	StructField("CREGCUSTLAT", StringType),
	StructField("CREGCUSTLON", StringType),
	StructField("CREGCUSTADRDT", StringType),
	StructField("COFFCLTIME", StringType),
	StructField("NDEFERREDDELIVERYDAYS", StringType),
	StructField("CFLFM", StringType),
	StructField("CCNTRYCODE", StringType),
	StructField("CSTATECODE", StringType),
	StructField("DCUSTEDD", StringType),
	StructField("CISDDN", StringType),
	StructField("CACTDELLOC", StringType),
	StructField("CGSTNO", StringType),
	StructField("CCUSTGSTNO", StringType),
	StructField("CCNEEGSTNO", StringType),
	StructField("CEXCHAWB", StringType),
	StructField("CCNEEFADD", StringType),
	StructField("DCUSTPUDT", StringType),
	StructField("CCUSTPUTM", StringType),
	StructField("CCONMOBMASK", StringType),
	StructField("CADDRESSTYPE", StringType),
	StructField("CAVAILTIME", StringType),
	StructField("CAVAILDAYS", StringType),
	StructField("CCSTMOBMAS", StringType),
	StructField("CRTOMOBMAS", StringType),
	StructField("COTPTYPE", StringType),
	StructField("NOTPNO", StringType),
	StructField("CINCOTERMS", StringType),
	StructField("CPURTCD", StringType),
	StructField("CPULOC", StringType),
	StructField("CCOMPGRP", StringType),
	StructField("CPACKAGINGID", StringType)))

  val shipcltschema = (new StructType).add("after", shipcltDF).add("current_ts", StringType)
    .add("op_ts", StringType)
    .add("op_type", StringType)
    .add("pos", StringType)
    .add("table", StringType)

  return shipcltschema
  
  }
 
  def getAwbmstSchema(sc:SparkContext):StructType = {
	val awbmstSchema1 = StructType(
        List(
		StructField("NAWBID", StringType),
		StructField("CAWBNO", StringType),
		StructField("CPRODCODE", StringType),
		StructField("CPRODTYPE", StringType),
		StructField("CORGAREA", StringType),
		StructField("CDSTAREA", StringType),
		StructField("CBATCHNO", StringType),
		StructField("NPCS", StringType),
		StructField("DSHIPDATE", StringType),
		StructField("DPUDATE", StringType),
		StructField("NTOKENNO", StringType),
		StructField("CPUWI", StringType),
		StructField("NDIMWEIGHT", StringType),
		StructField("CCUSTCODE", StringType),
		StructField("NCSHMEMNO", StringType),
		StructField("CCMDTYCODE", StringType),
		StructField("NCOLAMT", StringType),
		StructField("NAMT", StringType),
		StructField("CTRNCODE", StringType),
		StructField("CSPLINST", StringType),
		StructField("CPACKTYPE", StringType),
		StructField("CODA", StringType),
		StructField("CMODE", StringType),
		StructField("CGSACODE", StringType),
		StructField("CFODCODFLG", StringType),
		StructField("CFOCCODE", StringType),
		StructField("CBTPCODE", StringType),
		StructField("CBTPAREA", StringType),
		StructField("CBILLNO", StringType),
		StructField("BREFNO", StringType),
		StructField("BDUTY", StringType),
		StructField("CSECTYCK", StringType),
		StructField("NPARRENTCONTID", StringType),
		StructField("BBILLCNEE", StringType),
		StructField("CPUEMPCODE", StringType),
		StructField("BSTAMDART", StringType),
		StructField("CORGSCRCD", StringType),
		StructField("CDSTSCRCD", StringType),
		StructField("BPRIORITY", StringType),
		StructField("CSUBCODE", StringType),
		StructField("NCURLOOKUPID", StringType),
		StructField("CPUTIME", StringType),
		StructField("CFLFM", StringType),
		StructField("CDHLACCTNO", StringType),
		StructField("CRTOAREA", StringType),
		StructField("CRTOCODE", StringType),
		StructField("CEMAILID", StringType),
		StructField("NWEIGHT", StringType),
		StructField("NOCTROI", StringType),
		StructField("NDUTY", StringType),
		StructField("NSMARTBOXTYPEA", StringType),
		StructField("NSMARTBOXTYPEB", StringType),
		StructField("NDCCOUNT", StringType),
		StructField("CBILLCAC", StringType),
		StructField("BCRCRDREF", StringType),
		StructField("NDCRECD", StringType),
		StructField("CDLVPURTCD", StringType),
		StructField("CACCURACY", StringType),
		StructField("CDLCODE", StringType),
		StructField("CKGPOUND", StringType),
		StructField("CMLOCCODE", StringType),
		StructField("CADECODE", StringType),
		StructField("DINSCANDT", StringType),
		StructField("CINSCANLOC", StringType),
		StructField("BEDPTRNX", StringType),
		StructField("BCRCRDPAY", StringType),
		StructField("BIATADTL", StringType),
		StructField("CDOCATTACH", StringType),
		StructField("CEMPLCODE", StringType),
		StructField("NOPCS", StringType),
		StructField("CMUSTGOCRG", StringType),
		StructField("CODOCATTAC", StringType),
		StructField("CDATAENTRYLOC", StringType),
		StructField("BSHPCRCRDREF", StringType),
		StructField("DEPTDTDLV", StringType),
		StructField("BDETAIN", StringType),
		StructField("CISOVERAGE", StringType),
		StructField("CDHLFLAG", StringType),
		StructField("DDEMUDT", StringType),
		StructField("NDEMUAMT", StringType),
		StructField("CDEMUCODE", StringType),
		StructField("CDEMULOCCODE", StringType),
		StructField("DSTATUSDT", StringType),
		StructField("BCSBPRINTED", StringType),
		StructField("DDATAENTRYDT", StringType),
		StructField("DBATCHDT", StringType),
		StructField("CASTATTYPE", StringType),
		StructField("CASTATCODE", StringType),
		StructField("CSTATEMPLCODE", StringType),
		StructField("CAREMARKS", StringType),
		StructField("CAPTCODE", StringType),
		StructField("BPWPALLETIZED", StringType),
		StructField("CPRINTMODE", StringType),
		StructField("CPROMOCODE", StringType),
		StructField("CRTOIMTLY", StringType),
		StructField("BDGSHIPMENT", StringType),
		StructField("ISPWREC", StringType),
		StructField("CPREFTM", StringType),
		StructField("CREVPU", StringType),
		StructField("CPSCODE", StringType),
		StructField("CFRDAWBNO", StringType),
		StructField("CRFDCOMPNM", StringType),
		StructField("CREFNO2", StringType),
		StructField("CREFNO3", StringType),
		StructField("CPUMODE", StringType),
		StructField("CPUTYPE", StringType),
		StructField("NITEMCNT", StringType),
		StructField("BPARTIALPU", StringType),
		StructField("NPAYCASH", StringType),
		StructField("NPUTMSLOT", StringType),
		StructField("CMANIFSTNO", StringType),
		StructField("COFFCLTIME", StringType),
		StructField("NDEFERREDDELIVERYDAYS", StringType),
		StructField("DCUSTEDD", StringType),
		StructField("CISDDN", StringType),
		StructField("CACTDELLOC", StringType),
		StructField("CGSTNO", StringType),
		StructField("CVEHICLENO", StringType),
		StructField("CEXCHAWB", StringType),
		StructField("DPREFDATE", StringType),
		StructField("CPREFTIME", StringType),
		StructField("NDISTANCE", StringType),
		StructField("DCUSTPUDT", StringType),
		StructField("CCUSTPUTM", StringType),
		StructField("CAVAILTIME", StringType),
		StructField("CAVAILDAYS", StringType),
		StructField("COTPTYPE", StringType),
		StructField("NOTPNO", StringType),
		StructField("CPACKAGINGID", StringType),
		StructField("CCCODE", StringType)
		));
     val awbmstSchema = (new StructType).add("after", awbmstSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return awbmstSchema
  
  }

  def getDelsheetSchema(sc:SparkContext):StructType = {
	val delsheetSchema1 = StructType(
        List(
		StructField("DDATE", StringType),
		StructField("CEMPLCODE", StringType),
		StructField("NSRNO", StringType),
		StructField("CPRODCODE", StringType),
		StructField("CAWBNO", StringType),
		StructField("CMPSNO", StringType),
		StructField("DDELIVERYDT", StringType),
		StructField("CREMARKS", StringType),
		StructField("CRELATCODE", StringType),
		StructField("CRECIEVEDBY", StringType),
		StructField("CIDTYPE", StringType),
		StructField("CIDNUMBER", StringType),
		StructField("CLOCCODE", StringType),
		StructField("DPODEDATE", StringType),
		StructField("CPDECODE", StringType),
		StructField("BEDPTRNX", StringType),
		StructField("CSTATTYPE", StringType),
		StructField("CSTATCODE", StringType),
		StructField("CSTATTIME", StringType),
		StructField("CVEHICLENO", StringType),
		StructField("CDLVPURTCD", StringType),
		StructField("CINTLLOCATION", StringType),
		StructField("BPRINTED", StringType),
		StructField("CRELATION", StringType),
		StructField("CBATCHNO", StringType),
		StructField("NROUNDNO", StringType),
		StructField("CPREVAWBNO", StringType),
		StructField("CUNREGVEHICLENO", StringType),
		StructField("CCODE", StringType),
		StructField("CPSCODE", StringType),
		StructField("CEBNNOLASTMILE", StringType)
		));

      val delsheetSchema = (new StructType).add("after", delsheetSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return delsheetSchema
  
  }

  def getShipfltmstSchema(sc:SparkContext):StructType = {
	 val shipfltmstSchema1 = StructType(
        List(
		StructField("CAWBNO", StringType),
		StructField("CMPSNO", StringType),
		StructField("NRUNID", StringType),
		StructField("DRUNDATE", StringType),
		StructField("CRUNCODE", StringType),
		StructField("CORGAREA", StringType),
		StructField("CSTATTYPE", StringType),
		StructField("CSTATCODE", StringType),
		StructField("CEMPLCODE", StringType),
		StructField("CLOCCODE", StringType),
		StructField("NPARRENTCONTID", StringType),
		StructField("DDTINSCAN", StringType),
		StructField("CTALLY", StringType),
		StructField("BPRINTED", StringType),
		StructField("DSTATDATE", StringType),
		StructField("ISUNDELSHP", StringType),
		StructField("ISUNDELIVERDATE", StringType),
		StructField("ISNOTOUTSCAN", StringType),
		StructField("ISNOTOUTSCANDATE", StringType),
		StructField("CRECDATA", StringType),
		StructField("DOUTSCANDT", StringType),
		StructField("BOUTSCAN", StringType),
		StructField("BEDPTRNX", StringType),
		StructField("CDSTAREA", StringType),
		StructField("CPTALLY", StringType),
		StructField("NRPCS", StringType),
		StructField("CDOCATTACH", StringType),
		StructField("NOPCS", StringType),
		StructField("CFLIGHTNO", StringType),
		StructField("DFLIGHTDT", StringType),
		StructField("DLOADARRDATE", StringType),
		StructField("DFLTARRDATE", StringType),
		StructField("CVEHICLENO", StringType),
		StructField("NENDKMS", StringType),
		StructField("DPTALLYDATE", StringType),
		StructField("CACTFLIGHTNO", StringType),
		StructField("DACTFLIGHTDT", StringType),
		StructField("CTALLYSRC", StringType),
		StructField("DDSTARRDT", StringType),
		StructField("CDLVPURTCD", StringType),
		StructField("CTALLYPURTCD", StringType),
		StructField("NACTPARRENTCONTID", StringType),
		StructField("CSTATEMPLCODE", StringType),
		StructField("CSTATCLRACTION", StringType),
		StructField("CSTATCLREMPLCODE", StringType),
		StructField("DSTATCLRDATE", StringType),
		StructField("CMSTATCODE", StringType)));

      val shipfltmstSchema = (new StructType).add("after", shipfltmstSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return shipfltmstSchema
  
  }

  def getAwbaddressdetailsSchema(sc:SparkContext):StructType = {
	       val awbaddressdetailsSchema1 = StructType(
        List(
		StructField("NAWBID", StringType),
		StructField("CNAME", StringType),
		StructField("CCODE", StringType),
		StructField("CATTN", StringType),
		StructField("CPINCODE", StringType),
		StructField("CPHONENO", StringType),
		StructField("CMOBILE", StringType),
		StructField("CEMAILID", StringType),
		StructField("CFAX", StringType),
		StructField("CSHIPPER", StringType),
		StructField("NADDPREFERENCE", StringType),
		StructField("CADDRESS1", StringType),
		StructField("CADDRESS2", StringType),
		StructField("CADDRESS3", StringType),
		StructField("CCNTRYCODE", StringType),
		StructField("CSTATECODE", StringType),
		StructField("CGSTNO", StringType),
		StructField("CGPSLAT", StringType),
		StructField("CGPSLON", StringType),
		StructField("CDSOURCE", StringType),
		StructField("CCONMOBMASK", StringType),
		StructField("CADDRESSTYPE", StringType),
		StructField("CCSTMOBMAS", StringType),
		StructField("CRTOMOBMAS", StringType)
		));

      val awbaddressdetailsSchema = (new StructType).add("after", awbaddressdetailsSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return awbaddressdetailsSchema
  
  }

  def getCallpusSchema(sc:SparkContext):StructType = {
	  val callpusSchema1 = StructType(
      List(
		StructField("CAREA", StringType),
		StructField("CSCRCD", StringType),
		StructField("DREGDATE", StringType),
		StructField("CPRODCODE", StringType),
		StructField("CPRODTYPE", StringType),
		StructField("CTRNCODE", StringType),
		StructField("CCUSTCODE", StringType),
		StructField("CCUSTNAME", StringType),
		StructField("CCUSTADR1", StringType),
		StructField("CCUSTADR2", StringType),
		StructField("CCUSTADR3", StringType),
		StructField("CCUSTPIN", StringType),
		StructField("CCUSTTEL", StringType),
		StructField("CCONTACT", StringType),
		StructField("DPUDATE", StringType),
		StructField("CPUTIME", StringType),
		StructField("CPUPSDTM", StringType),
		StructField("CREMARKS", StringType),
		StructField("NPCS", StringType),
		StructField("NWEIGHT", StringType),
		StructField("NTOKENNO", StringType),
		StructField("CDLVPURTCD", StringType),
		StructField("CTRNXPSD", StringType),
		StructField("CPUPSDEMP", StringType),
		StructField("BPRINTED", StringType),
		StructField("CCUSTFAX", StringType),
		StructField("CMOBILENO", StringType),
		StructField("CPSDBYEMP", StringType),
		StructField("CLOGGEDBY", StringType),
		StructField("CREMINDER", StringType),
		StructField("DREMDATE", StringType),
		StructField("COFFCLTIME", StringType),
		StructField("CFOD", StringType),
		StructField("CCOD", StringType),
		StructField("CODA", StringType),
		StructField("BPRIORITY", StringType),
		StructField("CALTPURTCD", StringType),
		StructField("CPUEMPLCD", StringType),
		StructField("CALTPUEMP", StringType),
		StructField("NSRNO", StringType),
		StructField("CTDD", StringType),
		StructField("CMODE", StringType),
		StructField("DACTPUDATE", StringType),
		StructField("CACTPUTIME", StringType),
		StructField("CACTPUEMP", StringType),
		StructField("CRESCHPU", StringType),
		StructField("CEMAILID", StringType),
		StructField("CBOX", StringType),
		StructField("CSTATCODE", StringType),
		StructField("CSTATTYPE", StringType),
		StructField("CALTSCRCD", StringType),
		StructField("CREGISTERBY", StringType),
		StructField("CMODIFIEDBY", StringType),
		StructField("DMODIFIEDDATE", StringType),
		StructField("NVOLWEIGHT", StringType),
		StructField("CTOPAY", StringType),
		StructField("CPUSTATUS", StringType),
		StructField("ISLOCKED", StringType),
		StructField("LOCKINGUSER", StringType),
		StructField("CDTP", StringType),
		StructField("CSII", StringType),
		StructField("CIMPEXP", StringType),
		StructField("NUMBEROFBOX", StringType),
		StructField("CREASONREMARK", StringType),
		StructField("CATMPTIME", StringType),
		StructField("CAWBNO", StringType),
		StructField("NEWNTOKENNO", StringType),
		StructField("DREASSIGNEDDATE", StringType),
		StructField("CCALLERNAME", StringType),
		StructField("CCALLERPHONE", StringType),
		StructField("DRESCHDATE", StringType),
		StructField("BEDPTRNX", StringType),
		StructField("CFILENAME", StringType),
		StructField("CPUAREA", StringType),
		StructField("CISBULKUPLD", StringType),
		StructField("CISDEDONE", StringType),
		StructField("CREFNO", StringType),
		StructField("ETAIL", StringType),
		StructField("CRAWBNO", StringType),
		StructField("DSTATUSENTRYDT", StringType),
		StructField("CREVPU", StringType),
		StructField("NATTEMPTED", StringType),
		StructField("CCLSTATCODE", StringType),
		StructField("CCLSTATTYPE", StringType),
		StructField("CCLATMPTIME", StringType),
		StructField("CALLSTATUS", StringType),
		StructField("CISNEWPU", StringType),
		StructField("CISDDN", StringType),
		StructField("CGSTNO", StringType)));

      val callpusSchema = (new StructType).add("after", callpusSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return callpusSchema

 }
 
  def getawboperationSchema(sc:SparkContext):StructType = {
	  val awboperationSchema1 = StructType(
      List(
		StructField("NAWBOPID", StringType),
		StructField("NOPLOOKUPID", StringType),
		StructField("CLOCCODE", StringType),
		StructField("CAWBNO", StringType),
		StructField("CEMPLCODE", StringType),
		StructField("DOPERATIONDATE", StringType)))
		
      val awboperationSchema = (new StructType).add("after", awboperationSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return awboperationSchema

 }

  def getmdpickupSchema(sc:SparkContext):StructType = {
	  val mdpickupSchema1 = StructType(
      List(
		StructField("CPICKUPID", StringType),
		StructField("CAWBNO", StringType),
		StructField("CPRODCODE", StringType),
		StructField("CCUSTCODE", StringType),
		StructField("CORGAREA", StringType),
		StructField("CORGSCRCD", StringType),
		StructField("CDSTAREA", StringType),
		StructField("CDSTSCRCD", StringType),
		StructField("CSUBCODE", StringType),
		StructField("CSTATCODE", StringType),
		StructField("DSTATUSDATE", StringType),
		StructField("CMLOCCODE", StringType),
		StructField("CEMPLCODE", StringType),
		StructField("CDEVICENO", StringType),
		StructField("CSIMNO", StringType),
		StructField("CGPSLAT", StringType),
		StructField("CGPSLON", StringType),
		StructField("CGPSTIME", StringType),
		StructField("CGPSSATCNT", StringType),
		StructField("DUPLDDT", StringType),
		StructField("DSYNCDATE", StringType),
		StructField("CSTATUS", StringType),
		StructField("CPURTCODE", StringType),
		StructField("CISPARTIALPICKUP", StringType),
		StructField("CREASONOFPICKUPREJECTION", StringType),
		StructField("CSTATUSTYPE", StringType),
		StructField("CISPICKUPCANCELLED", StringType),
		StructField("NPUTMSLOT", StringType),
		StructField("CTYPE", StringType),
		StructField("DNPUDATE", StringType),
		StructField("CMDPICKUPDETID", StringType),
		StructField("CREMARKS", StringType)))
		
      val mdpickupSchema = (new StructType).add("after", mdpickupSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return mdpickupSchema

 }

  def getlocationtrackerSchema(sc:SparkContext):StructType = {
	  val loctrackSchema1 = StructType(
		List(
		StructField("BINBOUNDFLAG", StringType, true),
		StructField("BOUTBOUNDFLAG", StringType, true),
		StructField("CAWBNO", StringType, true),
		StructField("CAWBTRNX", StringType, true),
		StructField("CCONFLTTRNX", StringType, true),
		StructField("CDFILEFLAG", StringType, true),
		StructField("CEDPNAME", StringType, true),
		StructField("CLOCCODE", StringType, true),
		StructField("CMPSNO", StringType, true),
		StructField("CUSERAPPROVAL", StringType, true),
		StructField("DAPPROVALDTTM", StringType, true),
		StructField("DDFILEDATE", StringType, true),
		StructField("DEDPTRNX", StringType, true),
		StructField("DFILEDTTM", StringType, true),
		StructField("DFILENAME", StringType, true),
		StructField("DINDATE", StringType, true),
		StructField("NCCID", StringType, true),
		StructField("NTRACKERID", StringType, true),
		StructField("SEDPTRNX", StringType, true),
		StructField("SFILEDTTM", StringType, true),
		StructField("SFILENAME", StringType, true),
		StructField("op_ts", StringType, true),
		StructField("load_ts", TimestampType, true)
	))
		
      val loctrackSchema = (new StructType).add("after", loctrackSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return loctrackSchema

 }


  def getInboundSchema(sc:SparkContext):StructType = {
	  val inboundSchema1 = StructType(
		List(
		StructField("CAWBNO", StringType, true),
		StructField("CDSTAREA", StringType, true),
		StructField("CEMPLCODE", StringType, true),
		StructField("CLOCCODE", StringType, true),
		StructField("CMPSNO", StringType, true),
		StructField("CORGAREA", StringType, true),
		StructField("CPRODCODE", StringType, true),
		StructField("CREMARKS", StringType, true),
		StructField("CSTATCODE", StringType, true),
		StructField("CSTATTYPE", StringType, true),
		StructField("DENTDATE", StringType, true),
		StructField("DSTATDATE", StringType, true),
		StructField("NOPERATIONID", StringType, true),
		StructField("NSTATUSID", StringType, true),
		StructField("CFILENAME", StringType, true),
		StructField("SEDPTRNX", StringType, true),
		StructField("DEDPTRNX", StringType, true),
		StructField("DFILENAME", StringType, true),
		StructField("DLASTMODIFIEDTS", StringType, true),
		StructField("op_ts", StringType, true),
		StructField("load_ts", TimestampType, true)
		))
		
      val inboundSchema = (new StructType).add("after", inboundSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return inboundSchema

 }

  def getOutboundSchema(sc:SparkContext):StructType = {
	  val outboundSchema1 = StructType(
		List(
		StructField("CAWBNO", StringType, true),
		StructField("CDSTAREA", StringType, true),
		StructField("CEMPLCODE", StringType, true),
		StructField("CLOCCODE", StringType, true),
		StructField("CMPSNO", StringType, true),
		StructField("CORGAREA", StringType, true),
		StructField("CPRODCODE", StringType, true),
		StructField("CREMARKS", StringType, true),
		StructField("CSTATCODE", StringType, true),
		StructField("CSTATTYPE", StringType, true),
		StructField("DRUNDATE", StringType, true),
		StructField("DSTATDATE", StringType, true),
		StructField("NOPERATIONID", StringType, true),
		StructField("NSTATUSID", StringType, true),
		StructField("CFILENAME", StringType, true),
		StructField("SEDPTRNX", StringType, true),
		StructField("DEDPTRNX", StringType, true),
		StructField("CCNO", StringType, true),
		StructField("DLASTMODIFIEDTS", StringType, true),
		StructField("op_ts", StringType, true),
		StructField("load_ts", TimestampType, true)
		))
		
      val outboundSchema = (new StructType).add("after", outboundSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return outboundSchema

 }
 
 def getawblinkSchema(sc:SparkContext):StructType = {
	  val awblinkSchema1 = StructType(
		List(
		StructField("COAWBNO", StringType, true),
		StructField("COORGAREA", StringType, true),
		StructField("CODSTAREA", StringType, true),
		StructField("CNAWBNO", StringType, true),
		StructField("CNORGAREA", StringType, true),
		StructField("CNDSTAREA", StringType, true),
		StructField("NAMT", StringType, true),
		StructField("CPRODCODE", StringType, true),
		StructField("DDATE", StringType, true),
		StructField("DFLIGHTDT", StringType, true),
		StructField("CFLAG", StringType, true),
		StructField("CPRODTYPE", StringType, true),
		StructField("CFLIGHTNO", StringType, true),
		StructField("CLOCCODE", StringType, true),
		StructField("DDFILEDATE", StringType, true)
		))
		
      val awblinkSchema = (new StructType).add("after", awblinkSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return awblinkSchema

 }
 def getsryaltinstSchema(sc:SparkContext):StructType = {
	  val sryaltinstSchema1 = StructType(
		List(
		StructField("CTYPE", StringType, true),
		StructField("CEMPLCODE", StringType, true),
		StructField("CTIMEAREA", StringType, true),
		StructField("DATTEMPDT", StringType, true),
		StructField("CEMPLEMAIL", StringType, true),
		StructField("CEMPLNAME", StringType, true),
		StructField("CATTENTION", StringType, true),
		StructField("CAWBNO", StringType, true),
		StructField("CNAWBNO", StringType, true),
		StructField("DENTDATE", StringType, true),
		StructField("CENTTIME", StringType, true),
		StructField("CORGAREA", StringType, true),
		StructField("CDSTAREA", StringType, true),
		StructField("CPRODCODE", StringType, true),
		StructField("CCUSTCODE", StringType, true),
		StructField("CMODE", StringType, true),
		StructField("CMAILLOC", StringType, true),
		StructField("CREMARKS", StringType, true),
		StructField("CCUSTNAME", StringType, true),
		StructField("CTYP", StringType, true),
		StructField("BSRYALTINST", StringType, true),
		StructField("CRECDBY", StringType, true),
		StructField("CRELATION", StringType, true),
		StructField("CSTATCODE", StringType, true),
		StructField("CPUWI", StringType, true),
		StructField("DIMPORTDT", StringType, true)
		))
		
      val sryaltinstSchema = (new StructType).add("after", sryaltinstSchema1).add("current_ts", StringType)
        .add("op_ts", StringType)
        .add("op_type", StringType)
        .add("pos", StringType)
        .add("table", StringType)

  return sryaltinstSchema

 }
 

}

