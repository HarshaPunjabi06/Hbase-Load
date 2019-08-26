package com.ctower.util.schema
import org.apache.spark.sql.types.{StringType,LongType}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.broadcast.Broadcast
import org.apache.spark._


object SchemaHbase {
	
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

  
  def getShipcltSchema(sc:SparkContext): StructType = {
    val shipcltSchema = StructType(
    List(
      StructField("BBILLCNEE", StringType, true),
      StructField("BCHECKLST", StringType, true),
      StructField("BDOXATCHD", StringType, true),
      StructField("BLINKED", StringType, true),
      StructField("BPARTIALPU", StringType, true),
      StructField("BPRIORITY", StringType, true),
      StructField("BSTAMPDART", StringType, true),
      StructField("CACTDELLOC", StringType, true),
      StructField("CADDRESSTYPE", StringType, true),
      StructField("CADECODE", StringType, true),
      StructField("CADNLCRCRDREF", StringType, true),
      StructField("CALTATTN", StringType, true),
      StructField("CATTENTION", StringType, true),
      StructField("CAVAILDAYS", StringType, true),
      StructField("CAVAILTIME", StringType, true),
      StructField("CAWBFEE", StringType, true),
      StructField("CAWBNO", StringType, true),
      StructField("CBATCHNO", StringType, true),
      StructField("CBATORG", StringType, true),
      StructField("CBILLCAC", StringType, true),
      StructField("CBILLNO", StringType, true),
      StructField("CBTPAREA", StringType, true),
      StructField("CBTPCODE", StringType, true),
      StructField("CCALTADR1", StringType, true),
      StructField("CCALTADR2", StringType, true),
      StructField("CCALTADR3", StringType, true),
      StructField("CCALTADRDT", StringType, true),
      StructField("CCALTFAX", StringType, true),
      StructField("CCALTLAT", StringType, true),
      StructField("CCALTLON", StringType, true),
      StructField("CCALTMOB", StringType, true),
      StructField("CCALTNAME", StringType, true),
      StructField("CCALTPIN", StringType, true),
      StructField("CCALTTEL", StringType, true),
      StructField("CCARDCODE", StringType, true),
      StructField("CCARDHOLD", StringType, true),
      StructField("CCARTCHG", StringType, true),
      StructField("CCLECTYPE", StringType, true),
      StructField("CCMDTYCODE", StringType, true),
      StructField("CCMDTYDESC", StringType, true),
      StructField("CCNEEADR1", StringType, true),
      StructField("CCNEEADR2", StringType, true),
      StructField("CCNEEADR3", StringType, true),
      StructField("CCNEEADRDT", StringType, true),
      StructField("CCNEECODE", StringType, true),
      StructField("CCNEEFADD", StringType, true),
      StructField("CCNEEFAX", StringType, true),
      StructField("CCNEEGSTNO", StringType, true),
      StructField("CCNEELAT", StringType, true),
      StructField("CCNEELON", StringType, true),
      StructField("CCNEEMAIL", StringType, true),
      StructField("CCNEEMOB", StringType, true),
      StructField("CCNEENAME", StringType, true),
      StructField("CCNEEPIN", StringType, true),
      StructField("CCNEETEL", StringType, true),
      StructField("CCNTRYCODE", StringType, true),
      StructField("CCODFAVOUR", StringType, true),
      StructField("CCODPAYBLE", StringType, true),
      StructField("CCONMOBMASK", StringType, true),
      StructField("CCRCARDNO", StringType, true),
      StructField("CCRCRDREF", StringType, true),
      StructField("CCTMNO", StringType, true),
      StructField("CCUSTADR1", StringType, true),
      StructField("CCUSTADR2", StringType, true),
      StructField("CCUSTADR3", StringType, true),
      StructField("CCUSTADRDT", StringType, true),
      StructField("CCUSTCODE", StringType, true),
      StructField("CCUSTFAX", StringType, true),
      StructField("CCUSTGSTNO", StringType, true),
      StructField("CCUSTLAT", StringType, true),
      StructField("CCUSTLON", StringType, true),
      StructField("CCUSTMOB", StringType, true),
      StructField("CCUSTNAME", StringType, true),
      StructField("CCUSTPIN", StringType, true),
      StructField("CCUSTPUTM", StringType, true),
      StructField("CCUSTTEL", StringType, true),
      StructField("CDHLFLAG", StringType, true),
      StructField("CDIMEN1", StringType, true),
      StructField("CDIMEN2", StringType, true),
      StructField("CDIMEN3", StringType, true),
      StructField("CDLCODE", StringType, true),
      StructField("CDOCNO", StringType, true),
      StructField("CDSTAREA", StringType, true),
      StructField("CDSTSCRCD", StringType, true),
      StructField("CEMAILID", StringType, true),
      StructField("CEXCHAWB", StringType, true),
      StructField("CFLFM", StringType, true),
      StructField("CFLIGHTNO", StringType, true),
      StructField("CFOCCODE", StringType, true),
      StructField("CFODCODFLG", StringType, true),
      StructField("CFOVTYPE", StringType, true),
      StructField("CFRDAWBNO", StringType, true),
      StructField("CGSACODE", StringType, true),
      StructField("CGSTNO", StringType, true),
      StructField("CILLOC", StringType, true),
      StructField("CINVNO", StringType, true),
      StructField("CISDDN", StringType, true),
      StructField("CKGPOUND", StringType, true),
      StructField("CLOCCODE", StringType, true),
      StructField("CMANIFSTNO", StringType, true),
      StructField("CMAWBNO", StringType, true),
      StructField("CMISCCHG1", StringType, true),
      StructField("CMISCCHG2", StringType, true),
      StructField("CMLOCCODE", StringType, true),
      StructField("CMODE", StringType, true),
      StructField("CMRKSNOS1", StringType, true),
      StructField("CMRKSNOS2", StringType, true),
      StructField("CMRKSNOS3", StringType, true),
      StructField("COCTRCPTNO", StringType, true),
      StructField("CODA", StringType, true),
      StructField("COFFCLTIME", StringType, true),
      StructField("CORGAREA", StringType, true),
      StructField("CORGSCRCD", StringType, true),
      StructField("CPACKTYPE", StringType, true),
      StructField("CPAYTYPE", StringType, true),
      StructField("CPREFTM", StringType, true),
      StructField("CPRODCODE", StringType, true),
      StructField("CPRODDESC", StringType, true),
      StructField("CPRODTYPE", StringType, true),
      StructField("CPSCODE", StringType, true),
      StructField("CPUEMPLCD", StringType, true),
      StructField("CPUMODE", StringType, true),
      StructField("CPUTIME", StringType, true),
      StructField("CPUTYPE", StringType, true),
      StructField("CPUWI", StringType, true),
      StructField("CREFNO2", StringType, true),
      StructField("CREFNO3", StringType, true),
      StructField("CREGCUSTADR1", StringType, true),
      StructField("CREGCUSTADR2", StringType, true),
      StructField("CREGCUSTADR3", StringType, true),
      StructField("CREGCUSTADRDT", StringType, true),
      StructField("CREGCUSTLAT", StringType, true),
      StructField("CREGCUSTLON", StringType, true),
      StructField("CREGCUSTNAME", StringType, true),
      StructField("CREGCUSTPIN", StringType, true),
      StructField("CREGCUSTTEL", StringType, true),
      StructField("CREGTCHG", StringType, true),
      StructField("CREVPU", StringType, true),
      StructField("CRFDCOMPNM", StringType, true),
      StructField("CRTOADR1", StringType, true),
      StructField("CRTOADR2", StringType, true),
      StructField("CRTOADR3", StringType, true),
      StructField("CRTOADRDT", StringType, true),
      StructField("CRTOCONTNM", StringType, true),
      StructField("CRTOIMDTLY", StringType, true),
      StructField("CRTOLAT", StringType, true),
      StructField("CRTOLON", StringType, true),
      StructField("CRTOMOB", StringType, true),
      StructField("CRTOPIN", StringType, true),
      StructField("CRTOTEL", StringType, true),
      StructField("CSENDER", StringType, true),
      StructField("CSPLINST", StringType, true),
      StructField("CSTATCHG", StringType, true),
      StructField("CSTATECODE", StringType, true),
      StructField("CSUBCODE", StringType, true),
      StructField("CTRNCODE", StringType, true),
      StructField("DBATCHDT", StringType, true),
      StructField("DCUSTEDD", StringType, true),
      StructField("DCUSTPUDT", StringType, true),
      StructField("DDOCDATE", StringType, true),
      StructField("DDWNLDDATE", StringType, true),
      StructField("DEPTDTDLV", StringType, true),
      StructField("DFLIGHTDT", StringType, true),
      StructField("DINVDATE", StringType, true),
      StructField("DPUDATE", StringType, true),
      StructField("DSHIPDATE", StringType, true),
      StructField("DUPLDATE", StringType, true),
      StructField("DVALIDUPTO", StringType, true),
      StructField("NACTWGT", StringType, true),
      StructField("NADDISRCHG", StringType, true),
      StructField("NADDOSRCHG", StringType, true),
      StructField("NAMOUNT", StringType, true),
      StructField("NAMT", StringType, true),
      StructField("NASSDVALUE", StringType, true),
      StructField("NAWBFEE", StringType, true),
      StructField("NCARTCHG", StringType, true),
      StructField("NCDUTYPC", StringType, true),
      StructField("NCHGCOLCT", StringType, true),
      StructField("NCHRGWT", StringType, true),
      StructField("NCOLAMT", StringType, true),
      StructField("NCOMVAL", StringType, true),
      StructField("NCSHMEMNO", StringType, true),
      StructField("NDCCHRG", StringType, true),
      StructField("NDCLRDVAL", StringType, true),
      StructField("NDEFERREDDELIVERYDAYS", StringType, true),
      StructField("NDIMB", StringType, true),
      StructField("NDIMH", StringType, true),
      StructField("NDIML", StringType, true),
      StructField("NDIWT", StringType, true),
      StructField("NDOCCHRG", StringType, true),
      StructField("NDODAMT", StringType, true),
      StructField("NFODCHRG", StringType, true),
      StructField("NFREIGHT", StringType, true),
      StructField("NFSAMT", StringType, true),
      StructField("NITEMCNT", StringType, true),
      StructField("NMISCCHG1", StringType, true),
      StructField("NMISCCHG2", StringType, true),
      StructField("NOCTROI", StringType, true),
      StructField("NODACHRG", StringType, true),
      StructField("NOTHCHRGS", StringType, true),
      StructField("NPAYCASH", StringType, true),
      StructField("NPCS", StringType, true),
      StructField("NPUTMSLOT", StringType, true),
      StructField("NREGTCHG", StringType, true),
      StructField("NRISKHCHGS", StringType, true),
      StructField("NSLABWGT", StringType, true),
      StructField("NSTATCHG", StringType, true),
      StructField("NTOKENNO", StringType, true),
      StructField("NVALCHGS", StringType, true),
      StructField("NWEIGHT", StringType, true),
      StructField("op_ts", StringType, true),
      StructField("load_ts", StringType, true),
      StructField("op_flag", StringType, true)))
	  
    return shipcltSchema
  }
 
  def getawbmstSchema(sc:SparkContext): StructType = {
    val awbmstSchema = StructType(
    List(
		StructField("nawbid", StringType, true),
		StructField("cawbno", StringType, true),
		StructField("cprodcode", StringType, true),
		StructField("cprodtype", StringType, true),
		StructField("corgarea", StringType, true),
		StructField("cdstarea", StringType, true),
		StructField("cbatchno", StringType, true),
		StructField("npcs", StringType, true),
		StructField("dpudate", StringType, true),
		StructField("ntokenno", StringType, true),
		StructField("cpuwi", StringType, true),
		StructField("ndimweight", StringType, true),
		StructField("ncshmemno", StringType, true),
		StructField("ccmdtycode", StringType, true),
		StructField("namt", StringType, true),
		StructField("ctrncode", StringType, true),
		StructField("csplinst", StringType, true),
		StructField("cpacktype", StringType, true),
		StructField("coda", StringType, true),
		StructField("cmode", StringType, true),
		StructField("cgsacode", StringType, true),
		StructField("cfoccode", StringType, true),
		StructField("cbtpcode", StringType, true),
		StructField("cbtparea", StringType, true),
		StructField("cbillno", StringType, true),
		StructField("bbillcnee", StringType, true),
		StructField("cpuempcode", StringType, true),
		StructField("corgscrcd", StringType, true),
		StructField("cdstscrcd", StringType, true),
		StructField("bpriority", StringType, true),
		StructField("csubcode", StringType, true),
		StructField("cputime", StringType, true),
		StructField("nweight", StringType, true),
		StructField("cbillcac", StringType, true),
		StructField("ckgpound", StringType, true),
		StructField("cadecode", StringType, true),
		StructField("dinscandt", StringType, true),
		StructField("bcrcrdpay", StringType, true),
		StructField("biatadtl", StringType, true),
		StructField("cdataentryloc", StringType, true),
		StructField("bshpcrcrdref", StringType, true),
		StructField("deptdtdlv", StringType, true),
		StructField("dbatchdt", StringType, true),
		StructField("cpromocode", StringType, true),
		StructField("cpreftm", StringType, true),
		StructField("crevpu", StringType, true),
		StructField("cpscode", StringType, true),
		StructField("cfrdawbno", StringType, true),
		StructField("crfdcompnm", StringType, true),
		StructField("crefno2", StringType, true),
		StructField("crefno3", StringType, true),
		StructField("cpumode", StringType, true),
		StructField("cputype", StringType, true),
		StructField("nitemcnt", StringType, true),
		StructField("bpartialpu", StringType, true),
		StructField("npaycash", StringType, true),
		StructField("nputmslot", StringType, true),
		StructField("cmanifstno", StringType, true),
		StructField("coffcltime", StringType, true),
		StructField("ndeferreddeliverydays", StringType, true),
		StructField("dcustedd", StringType, true),
		StructField("cisddn", StringType, true),
		StructField("cactdelloc", StringType, true),
		StructField("cgstno", StringType, true),
		StructField("cexchawb", StringType, true),
		StructField("dcustpudt", StringType, true),
		StructField("ccustputm", StringType, true),
		StructField("cavailtime", StringType, true),
		StructField("cavaildays", StringType, true),
		StructField("load_ts", TimestampType, true)
	))

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

  def getawbadressSchema(sc:SparkContext): StructType = {
    val awbaddressSchema = StructType(
    List(
	StructField("nawbid", StringType, true),
	StructField("ccode", StringType, true),
	StructField("cattn", StringType, true),
	StructField("cshipper", StringType, true),
	StructField("op_ts", StringType, true),
	StructField("load_ts", TimestampType, true)
	))

    return awbaddressSchema
  }

  def getCallpusSchema(sc:SparkContext):StructType = {
	  val callpusSchema1 = StructType(
      List(
		StructField("carea", StringType),
		StructField("cscrcd", StringType),
		StructField("dregdate", StringType),
		StructField("cprodcode", StringType),
		StructField("cprodtype", StringType),
		StructField("ctrncode", StringType),
		StructField("ccustcode", StringType),
		StructField("ccustname", StringType),
		StructField("ccustadr1", StringType),
		StructField("ccustadr2", StringType),
		StructField("ccustadr3", StringType),
		StructField("ccustpin", StringType),
		StructField("ccusttel", StringType),
		StructField("ccontact", StringType),
		StructField("dpudate", StringType),
		StructField("cputime", StringType),
		StructField("cpupsdtm", StringType),
		StructField("cremarks", StringType),
		StructField("npcs", StringType),
		StructField("nweight", StringType),
		StructField("ntokenno", StringType),
		StructField("cdlvpurtcd", StringType),
		StructField("ctrnxpsd", StringType),
		StructField("cpupsdemp", StringType),
		StructField("bprinted", StringType),
		StructField("ccustfax", StringType),
		StructField("cmobileno", StringType),
		StructField("cpsdbyemp", StringType),
		StructField("cloggedby", StringType),
		StructField("creminder", StringType),
		StructField("dremdate", StringType),
		StructField("coffcltime", StringType),
		StructField("cfod", StringType),
		StructField("ccod", StringType),
		StructField("coda", StringType),
		StructField("bpriority", StringType),
		StructField("caltpurtcd", StringType),
		StructField("cpuemplcd", StringType),
		StructField("caltpuemp", StringType),
		StructField("nsrno", StringType),
		StructField("ctdd", StringType),
		StructField("cmode", StringType),
		StructField("dactpudate", StringType),
		StructField("cactputime", StringType),
		StructField("cactpuemp", StringType),
		StructField("creschpu", StringType),
		StructField("cemailid", StringType),
		StructField("cbox", StringType),
		StructField("cstatcode", StringType),
		StructField("cstattype", StringType),
		StructField("caltscrcd", StringType),
		StructField("cregisterby", StringType),
		StructField("cmodifiedby", StringType),
		StructField("dmodifieddate", StringType),
		StructField("nvolweight", StringType),
		StructField("ctopay", StringType),
		StructField("cpustatus", StringType),
		StructField("islocked", StringType),
		StructField("lockinguser", StringType),
		StructField("cdtp", StringType),
		StructField("csii", StringType),
		StructField("cimpexp", StringType),
		StructField("numberofbox", StringType),
		StructField("creasonremark", StringType),
		StructField("catmptime", StringType),
		StructField("cawbno", StringType),
		StructField("newntokenno", StringType),
		StructField("dreassigneddate", StringType),
		StructField("ccallername", StringType),
		StructField("ccallerphone", StringType),
		StructField("dreschdate", StringType),
		StructField("bedptrnx", StringType),
		StructField("cfilename", StringType),
		StructField("cpuarea", StringType),
		StructField("cisbulkupld", StringType),
		StructField("cisdedone", StringType),
		StructField("crefno", StringType),
		StructField("etail", StringType),
		StructField("crawbno", StringType),
		StructField("dstatusentrydt", StringType),
		StructField("crevpu", StringType),
		StructField("nattempted", StringType),
		StructField("cclstatcode", StringType),
		StructField("cclstattype", StringType),
		StructField("cclatmptime", StringType),
		StructField("callstatus", StringType),
		StructField("cisnewpu", StringType),
		StructField("cisddn", StringType),
		StructField("cgstno", StringType),
		StructField("op_ts", StringType, true),
		StructField("load_ts", TimestampType, true)));
      
  return callpusSchema1

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
		StructField("cpickupid", StringType, true),
		StructField("cawbno", StringType, true),
		StructField("cprodcode", StringType, true),
		StructField("ccustcode", StringType, true),
		StructField("corgarea", StringType, true),
		StructField("corgscrcd", StringType, true),
		StructField("cdstarea", StringType, true),
		StructField("cdstscrcd", StringType, true),
		StructField("csubcode", StringType, true),
		StructField("cstatcode", StringType, true),
		StructField("dstatusdate", StringType, true),
		StructField("cmloccode", StringType, true),
		StructField("cemplcode", StringType, true),
		StructField("cdeviceno", StringType, true),
		StructField("csimno", StringType, true),
		StructField("cgpslat", StringType, true),
		StructField("cgpslon", StringType, true),
		StructField("cgpstime", StringType, true),
		StructField("cgpssatcnt", StringType, true),
		StructField("duplddt", StringType, true),
		StructField("dsyncdate", StringType, true),
		StructField("cstatus", StringType, true),
		StructField("cpurtcode", StringType, true),
		StructField("cispartialpickup", StringType, true),
		StructField("creasonofpickuprejection", StringType, true),
		StructField("cstatustype", StringType, true),
		StructField("cispickupcancelled", StringType, true),
		StructField("nputmslot", StringType, true),
		StructField("ctype", StringType, true),
		StructField("dnpudate", StringType, true),
		StructField("cmdpickupdetid", StringType, true),
		StructField("cremarks", StringType, true),
		StructField("op_ts", StringType, true),
		StructField("load_ts", TimestampType, true)
		));

  return mdpickupSchema1

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


  def getinboundSchema(sc:SparkContext): StructType = {
    val inboundSchema = StructType(
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

    return inboundSchema
  }

  def getoutboundSchema(sc:SparkContext): StructType = {
    val outboundSchema = StructType(
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
		StructField("DDFILEDATE", StringType, true),
		StructField("op_ts", StringType, true),
		StructField("load_ts", TimestampType, true)
		))
		
  return awblinkSchema1

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
		StructField("DIMPORTDT", StringType, true),
		StructField("op_ts", StringType, true),
		StructField("load_ts", TimestampType, true)
		))


  return sryaltinstSchema1

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
  def getmdpodSchema(sc:SparkContext):StructType = {
  val mdpodSchema1 = StructType(
    List(
      StructField("CMPODID", StringType, true),
      StructField("CMPODDETID", StringType, true),
      StructField("CPRODCODE", StringType, true),
      StructField("CORGAREA", StringType, true),
      StructField("CDSTAREA", StringType, true),
      StructField("CSTATTYPE", StringType, true),
      StructField("CSTATCODE", StringType, true),
      StructField("CAWBNO", StringType, true),
      StructField("DSTATDATE", StringType, true),
      StructField("DSTATTIME", StringType, true),
      StructField("CEMPLCODE", StringType, true),
      StructField("CRECDBY", StringType, true),
      StructField("CDSTSCRCD", StringType, true),
      StructField("CRELATION", StringType, true),
      StructField("CREMARKS", StringType, true),
      StructField("CIDTYPE", StringType, true),
      StructField("CIDNO", StringType, true),
      StructField("CDEVICENO", StringType, true),
      StructField("CSIMNO", StringType, true),
      StructField("CGPSLAT", StringType, true),
      StructField("CGPSLON", StringType, true),
      StructField("DTRACK_INSERT", StringType, true),
      StructField("DTRACK_UPDATE", StringType, true),
      StructField("CSTATTIME", StringType, true),
      StructField("CAREA", StringType, true),
      StructField("CLOCCODE", StringType, true),
      StructField("DEDPUPDDT", StringType, true),
      StructField("DSYNCDATE", StringType, true),
      StructField("CSTATUS", StringType, true),
      StructField("CGPSTIME", StringType, true),
      StructField("CGPSSATCNT", StringType, true)
    ))

  val mdpodSchema = (new StructType).add("after", mdpodSchema1).add("current_ts", StringType)
    .add("op_ts", StringType)
    .add("op_type", StringType)
    .add("pos", StringType)
    .add("table", StringType)

  return mdpodSchema
}

 

}

