import java.net.URLEncoder

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.http.HTTPConn.HTTPImplicit
import com.github.music.of.the.ainur.almaren.jdbc.JDBC.JDBCImplicit

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
// Spark 3.3+ uses log4j 2 (log4j 1 `Logger.setLevel` is gone), so use the log4j2 API
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator

import scala.io.Source
import com.github.music.of.the.ainur.almaren.http.Response
import spark.implicits._

import scala.util.Try
import scala.util.{Failure, Success}
import com.modak.common.credential.Credential
import com.modak.common.ldap
import com.modak.common.CredentialPayload

import com.ibm.icu.util.TimeZone
import java.time.{ZoneId, ZonedDateTime}

Configurator.setLevel("com.github.music.of.the.ainur.almaren", Level.INFO)
val almaren = Almaren("googleGeoCode").builder
Configurator.setLevel("com.googleGeoCode", Level.INFO)
val logger = LogManager.getLogger("com.googleGeoCode")

case class urlBucket(`__ID__`: String, `__URL__`: String)

object googleGeoCode{

  val credential_api_url = sc.getConf.contains("spark.nabu.fireshots_url") match{
    case true => sc.getConf.get("spark.nabu.fireshots_url")
    case false => logger.error(s"credential_api_url config is not available")
      sys.exit(1)
  } 
  val token = sc.getConf.contains("spark.nabu.token") match{
    case true => sc.getConf.get("spark.nabu.token")
    case false => logger.error(s"token config is not available")
      sys.exit(1)
  }
  val geoCodeUrl = sc.getConf.contains("spark.driver.geoCodeUrl") match{
    case true => sc.getConf.get("spark.driver.geoCodeUrl")
    case false => logger.error(s"geoCodeUrl config is not available")
      sys.exit(1)
  }
    logger.info(s"[+] geoCodeUrl : $geoCodeUrl")
	
  val timezoneUrl = sc.getConf.contains("spark.driver.timezoneUrl") match{
    case true => sc.getConf.get("spark.driver.timezoneUrl")
    case false => logger.error(s"timezoneUrl config is not available")
      sys.exit(1)
  }
    logger.info(s"[+] timezoneUrl : $timezoneUrl")
	
  val schema = sc.getConf.contains("spark.driver.schema") match{
    case true => sc.getConf.get("spark.driver.schema")
    case false => logger.error(s"schema config is not available")
      sys.exit(1)
  }
    logger.info(s"[+] schema : $schema")
	
  val oracleURL = sc.getConf.contains("spark.driver.oracleURL") match{
    case true => sc.getConf.get("spark.driver.oracleURL")
    case false => logger.error(s"oracleURL config is not available")
      sys.exit(1)
  }
    logger.info(s"[+] oracleURL : $oracleURL")
	
  val applicationStatusAPIUrl = sc.getConf.contains("spark.driver.applicationStatusAPIUrl") match{
    case true => sc.getConf.get("spark.driver.applicationStatusAPIUrl")
    case false => logger.error(s"applicationStatusAPIUrl config is not available")
       sys.exit(1)
  }
   logger.info(s"[+] applicationStatusAPIUrl : $applicationStatusAPIUrl")

  def getCredentials(api: String, token: String, credential_id: Int, credential_type_id: Int): ldap = {
    val CredentialResult = Credential.getCredentialData(
      CredentialPayload(token, credential_id, credential_type_id, api)
    )
    val ldap = CredentialResult.data match {
      case ldap: ldap => ldap
      case _ => throw new Exception("Currently unavailable for other credentials Types")
    }
    ldap
  }
  val oracle_cred = sc.getConf.contains("spark.driver.oracle_cred") match{
    case true => sc.getConf.get("spark.driver.oracle_cred").split(",")
    case false => logger.error(s"Oracle Credentials config is not available")
      sys.exit(1)
  }
  val access_key_cred = sc.getConf.contains("spark.driver.access_key_cred") match{
    case true => sc.getConf.get("spark.driver.access_key_cred").split(",")
    case false => logger.error(s"Access key config is not available")
      sys.exit(1)
  }
  val workload_conf = sc.getConf.contains("spark.driver.workload_cred") match {
    case true => sc.getConf.get("spark.driver.workload_cred").split(",")
    case false => logger.error(s"Workload Credentials config is not available")
      sys.exit(1)
  }
  val oracleCredentials = getCredentials(credential_api_url, token, oracle_cred(0).toInt, oracle_cred(1).toInt)
  val accessKeyCredentials = getCredentials(credential_api_url, token, access_key_cred(0).toInt, access_key_cred(1).toInt)
  val workloadCredentials = getCredentials(credential_api_url, token, workload_conf(0).toInt, workload_conf(1).toInt)
  
  val username = oracleCredentials.username
  val password = oracleCredentials.password
  
  val access_key = accessKeyCredentials.password
  
  val workload_username = workloadCredentials.username
  val workload_password = workloadCredentials.password
  
  val jdbcDriver = "oracle.jdbc.OracleDriver"

  val COMPLETED = "COMPLETED"
  val ERROR = "ERROR"
  val RUNNING = "RUNNING"
  
  def uuid = java.util.UUID.randomUUID.toString	
  val jobId = uuid
  
  var startTimeUnix = spark.sql("select unix_timestamp()").first.getLong(0)
  val config = ConfigFactory.load("google.conf")
  
  val batchMode = config.hasPath("com.ingestion.batch_mode") match{
    case true => config.getBoolean("com.ingestion.batch_mode")
    case false => false
  }
   logger.info(s"[+] batchMode : $batchMode")
  
  def fileRead(filename: String): String = {
    Source.fromFile(s"code/geoCode/files/$filename").getLines.mkString("\n")
  }
  
  val job = config.getString("com.ingestion.oracleTableName")
  val geoCodeschema = config.getString("com.ingestion.geoCodeschema")
  val timezoneschema = config.getString("com.ingestion.timezoneschema")
  val checkpointPath = config.getString("com.ingestion.checkpointPath")
  
  val sql_select = config.getString("com.ingestion.sql")
  val sql_query = sql_select + schema + "." + job

  val stgTable = job + "_STG"

  val geoCodedsl = fileRead(s"$job/geoCodedsl.dsl")
  val timeZonedsl = fileRead(s"$job/timeZonedsl.dsl")
  val geoCodeMergeFile = fileRead(s"$job/geoCode.merge").stripMargin
  val timeZoneMergeFile = fileRead(s"$job/timeZone.merge").stripMargin
  val checkpointValue = config.getString("com.ingestion.checkpoint_value")
  
  def readCheckPoint: Option[String] =
    Try(spark.read.parquet(checkpointPath).selectExpr("max(checkpoint)").as[Option[String]].first).getOrElse(None)

  var checkpoint: Option[String] = readCheckPoint
  logger.info(s"[+] Checkpoint : $checkpoint")

  def encodeBase64(str: String): String =
    java.util.Base64.getEncoder.encodeToString(str.getBytes())
		
  
  def getoracleDf(query: String): DataFrame= {
    logger.info("[+] Getting OracleDataFrame ")
    almaren
      .sourceJdbc(oracleURL, jdbcDriver, query, Some(username), Some(password))
      .cache()
      .batch
  }
  
  def newCheckpoint(df : DataFrame): Dataset[Option[String]]={
    logger.info("[+] Generating new checkpoint value dataset")
    batchMode match {
      case true =>
        almaren
	  .sourceSql("""select date_format("","yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ") as checkpoint""")
	  .batch.as[Option[String]]
	   
      case false =>
        almaren
	  .sourceDataFrame(df)
	  .sqlExpr(s"cast(max(updt_ts) as string) as checkpoint")
	  .batch.as[Option[String]]
    }
  }

  def writeCheckPoint(ds: Dataset[Option[String]]): Option[String] = {
    logger.info(s"[+] Writing Checkpoint ${ds.show(false)}")
    ds.write.format("parquet").mode("append").save(checkpointPath)
    ds.first
  }
  
  def updateCheckpoint(checkpointDataSet: Dataset[Option[String]]): Unit = {
    logger.info("[+] Updating Checkpoint File")
     checkpoint = checkpointDataSet match {
       case ds if !ds.first.isEmpty && ds.first != checkpoint => writeCheckPoint(ds)
       case _ => checkpoint
     }
  }
  
  def getAllData[T](allPages: Dataset[T]): Dataset[Response] = {
    logger.info("[+] Getting Entire Data For an Object ")
    almaren
      .sourceDataFrame(allPages)
      .repartition(100)
      .http(headers = Map("Content-Type" -> "application/x-www-form-urlencoded"), method = "GET", readTimeout = 240000, connectTimeout = 240000)
      .cache()
      .batch.as[Response]
  }
  
  def geoCodeUrlGenerator(final_address_df: DataFrame): IndexedSeq[urlBucket] = {
    
    final_address_df.collect().map { row =>
      val id = row.getAs[Long]("__ID__")
      val parameter = row.getAs[String]("final_address")
      val parameterList = List(parameter)
      val encodedParameter = parameterList.map(a => URLEncoder.encode(a, "UTF-8")).mkString(",")
      val url = geoCodeUrl + "?address=" + encodedParameter + "&key=" + access_key
      urlBucket(id.toString, url)
    }.toIndexedSeq
 }
  
  def getGeoCodeResponseData(result: Dataset[Response]): DataFrame = {
    logger.info("[+] Getting Success Data For an Object ")
    var deserialized_df = almaren
			   .sourceDataFrame(result)
			   .deserializer("JSON", "__BODY__", Some(geoCodeschema))
			   .dsl(geoCodedsl)
			   .batch
    deserialized_df = deserialized_df.dropDuplicates("__ID__")
    val address_components_df = getGeoCodeAddressComponentData(result)
    val fullData_df = deserialized_df.join(address_components_df, Seq("__ID__"),"right").cache()
    fullData_df
  }
  
  def getGeoCodeAddressComponentData(result_df: Dataset[Response]): DataFrame = {
    logger.info("[+] Getting Address Components Data For an Object ")
    val deserialized_result_df = almaren.sourceDataFrame(result_df).alias("__TABLE__").sql("select __ID__,__BODY__ from __TABLE__").deserializer("JSON", "__BODY__", None).batch
    val explode_results_df = deserialized_result_df.select($"__ID__",explode($"results").as("tmp")).select("__ID__","tmp.*").dropDuplicates("__ID__")
    val explode_address_components_df = explode_results_df.select($"__ID__",explode($"address_components").as("tmp")).select("__ID__","tmp.*")
    val types_new_df = explode_address_components_df.withColumn("types_new",concat_ws(",",col("types"))).withColumn("new",to_json(struct($"long_name", $"short_name", $"types"))).drop("long_name","short_name","types")
    val address_components_df = almaren.sourceDataFrame(types_new_df).alias("__TABLE__").sql("select __ID__,max(case when types_new ='postal_code' then new end) AS g_address_components_postal_code, max(case when types_new ='locality,political' then new end) AS g_address_components_locality,  max(case when types_new ='country,political' then new end) AS g_address_components_country,  max(case when types_new ='administrative_area_level_1,political' then new end) AS g_address_components_administrative_area_level_1 from __TABLE__ group by __ID__").batch
    address_components_df
  }
  
  def executeJdbcQuery(query: String): Unit = {
    logger.info(s"[+] Query Statement: {$query}")
    almaren
      .jdbcQuery(oracleURL, jdbcDriver, query, Some(username), Some(password), Map("connectionTimeoutMillis" -> "15000", "maxSize" -> "1"))
      .batch
  }
 
  def targetJdbcData(df: DataFrame): DataFrame = {
    logger.info("[+] Writing into the StagingTable ")
    almaren.sourceDataFrame(df).targetJdbc(oracleURL, jdbcDriver, stgTable, SaveMode.Append, Some(username), Some(password)).batch
  }
  
  def insertErrorAudit(df: DataFrame, processName: String): Unit = {
    df.show(false)
    almaren
      .sourceDataFrame(df)
      .alias("__TABLE__")
      .sql(s"""select '$jobId' as JOB_ID,__BODY__ as ERROR_MESSAGE,'$processName' as STAGE_NAME,__URL__ as PARAMETERS,concat(STUDY_NUM,",",SITE_NUM) as UNIQUE_KEY_DATA from __TABLE__""")
      .targetJdbc(oracleURL, jdbcDriver, "T_NABU_ERROR_AUDIT", SaveMode.Append, Some(username), Some(password), Map("batchsize" -> "5000"))
      .batch
  }
  
  def timeZoneUrlGenerator(df: DataFrame) : IndexedSeq[urlBucket] = {
    df.collect().map { row =>
      val id = row.getAs[Long]("__ID__")
      val parameter = row.getAs[String]("LOCATION")
      val parameterList = List(parameter)
      val encodedParameter = parameterList.map(a => URLEncoder.encode(a, "UTF-8")).mkString(",")
      val url = timezoneUrl + "?location=" + encodedParameter + "&timestamp=" + startTimeUnix.toString + "&key=" + access_key
      urlBucket(id.toString, url)
    }.toIndexedSeq
  }
  
  def getTimeZoneResponseData(result: Dataset[Response]): DataFrame = {
    logger.info("[+] Getting Success Data For an TimeZone Object ")
    almaren
	.sourceDataFrame(result)
	.deserializer("JSON", "__BODY__", Some(timezoneschema))
	.dsl(timeZonedsl)
	.batch
  }
  
  def retryErrorData[T](failureData: Dataset[Response],parameterDF: DataFrame): DataFrame = {
    var failureCount = failureData.count.toInt
    var retrySuccessData = spark.emptyDataFrame
    var retryFailureData = spark.emptyDataFrame
    var errorDataFrame = failureData.select("__ID__","__URL__")
    var i =1
    while(failureCount>0){
       logger.info(s"[+] Starting retry process for ${i}th time")
       var retryData =getAllData(errorDataFrame)
       var retryGoodData = retryData.where("""get_json_object(__BODY__,'$.status') = "OK" and __STATUS_CODE__ == 200""")
       var retryBadData = retryData.where("""get_json_object(__BODY__,'$.status') == "OVER_QUERY_LIMIT"""")
       retrySuccessData = retrySuccessData.isEmpty match{
         case true => retryGoodData.toDF
	 case false => retrySuccessData.union(retryGoodData.toDF)
       }
       retryFailureData = retryFailureData.isEmpty match{
         case true => retryData.where("""__STATUS_CODE__ <> 200 or get_json_object(__BODY__,'$.status') == "ZERO_RESULTS"""").toDF
	 case false => retryFailureData.union(retryData.where("""__STATUS_CODE__ <> 200 or get_json_object(__BODY__,'$.status') == "ZERO_RESULTS"""").toDF)
       }
       errorDataFrame = retryBadData.select("__ID__","__URL__")
       failureCount = retryBadData.count.toInt
       i=i+1
       logger.info(s"[+] Records to be reprocessed:${failureCount}")
    }
    if(retryFailureData.count>0){
	logger.info(s"[+] loading the zero result params to error audit")
	insertErrorAudit(parameterDF.join(retryFailureData, Seq("__ID__"),"right"),"retryProcess")
    }
    retrySuccessData
  }
  
  def geoCodeData(df: DataFrame):DataFrame={
    logger.info(s"[+] Getting GeoCode Data")
    val geoCodeSource_df = almaren
	                     .sourceDataFrame(df)
			     .sqlExpr("STUDY_NUM","SITE_NUM","CNTRY_NM","ZIP_PSTL_CD","CITY","STATE_PROVINCE","ADDRESS_FULL")
			     .cache()
			     .batch
    val id_df = almaren.sourceDataFrame(geoCodeSource_df).alias("__TABLE__").sql(s"select monotonically_increasing_id() as __ID__, * from __TABLE__").cache().batch
    val null_remove_df = id_df.na.fill("null")
    val paramDF = null_remove_df.select("__ID__","STUDY_NUM","SITE_NUM")
    logger.info("[+] Inserting FINAL_ADDRESS Column")
    val final_address_df = almaren.sourceDataFrame(null_remove_df).alias("__TABLE__").sql("select *,ADDRESS_FULL||','||CITY||','||STATE_PROVINCE||','||ZIP_PSTL_CD||','||CNTRY_NM as final_address from __TABLE__").batch
    val geoCodeAllPages = geoCodeUrlGenerator(final_address_df.where("final_address is not null"))
    val geoCodeData =getAllData(geoCodeAllPages.toDS)
    val successGeoCodeData = geoCodeData.where("""get_json_object(__BODY__,'$.status') = "OK"  and __STATUS_CODE__ == 200""")
    val successGeoCodeDataCount = successGeoCodeData.count
    logger.info(s"[+] Success data count after initial request:${successGeoCodeDataCount}")
    val retryGeoCodeData = geoCodeData.where("""get_json_object(__BODY__,'$.status') == "OVER_QUERY_LIMIT"""")
    val retryGeoCodeDataCount = retryGeoCodeData.count
    logger.info(s"[+] No of records to retry after initial request:${retryGeoCodeDataCount}")
    val erroredData = geoCodeData.where("""__STATUS_CODE__ <> 200 or get_json_object(__BODY__,'$.status') == "ZERO_RESULTS"""")
    if(erroredData.count>0){
       logger.info(s"[+] writing the invali requests and zero result requests to error audit")
       insertErrorAudit(paramDF.join(erroredData, Seq("__ID__"),"right"),"geoCode")
    }
    val finalGeoCodeResponseData = retryGeoCodeData.count match {
	case 0 => 
	   getGeoCodeResponseData(successGeoCodeData)
	case _ =>
	   val retryGeoCodeResponse = retryErrorData(retryGeoCodeData,paramDF)
	   val successGeoCodeResponseData = successGeoCodeData.union(retryGeoCodeResponse.as[Response])
	   getGeoCodeResponseData(successGeoCodeResponseData)
    }
    finalGeoCodeResponseData.join(id_df, Seq("__ID__"),"right").drop("CNTRY_NM","ZIP_PSTL_CD","CITY","STATE_PROVINCE","ADDRESS_FULL","__ID__").cache()
  }

  def timeZoneData(df: DataFrame):DataFrame={
    logger.info(s"[+] Getting TimeZone Data")
    val tz_id_df = almaren.sourceDataFrame(df).alias("__TABLE__").sql(s"select monotonically_increasing_id() as __ID__, * from __TABLE__").cache().batch
    val location_df = almaren.sourceDataFrame(tz_id_df).alias("__TABLE__").sql("select *,G_GEOMETRY_LOCATION_LAT||','||G_GEOMETRY_LOCATION_LNG as LOCATION from __TABLE__").batch
    val paramDF = location_df.select("__ID__","STUDY_NUM","SITE_NUM")
    val timezone_df = location_df.select("__ID__","LOCATION").where("LOCATION is not null")
    logger.info("[+] Generating all the TimeZone URLS")
    val allPages = timeZoneUrlGenerator(timezone_df)
    val allTimeZoneData =getAllData(allPages.toDS)
    val successTimeZoneData = allTimeZoneData.where("""get_json_object(__BODY__,'$.status') = "OK" and __STATUS_CODE__ == 200""")
    val successTimeZoneDataCount = successTimeZoneData.count
    logger.info(s"[+] Success data count after initial request:${successTimeZoneDataCount}")
    val retryTimeZoneData = allTimeZoneData.where("""get_json_object(__BODY__,'$.status') == "OVER_QUERY_LIMIT"""")
    val retryTimeZoneDataCount = retryTimeZoneData.count
    logger.info(s"[+] No of records to retry after initial request:${retryTimeZoneDataCount}")
    val erroredData = allTimeZoneData.where("""__STATUS_CODE__ <> 200 or get_json_object(__BODY__,'$.status') == "ZERO_RESULTS"""")
    if(erroredData.count>0){
       logger.info(s"[+] writing the failed and zero result requests to error audit")
       insertErrorAudit(paramDF.join(erroredData, Seq("__ID__"),"right"),"timeZone")
    }
    val finaltimeZoneResponseData = retryTimeZoneData.count match {
	case 0 =>
	    getTimeZoneResponseData(successTimeZoneData)
	case _ =>
	    val retryTimeZoneResponse = retryErrorData(retryTimeZoneData,paramDF)
	    val successTimeZoneResponseData = successTimeZoneData.union(retryTimeZoneResponse.as[Response])
	    getTimeZoneResponseData(successTimeZoneResponseData)
    }
    finaltimeZoneResponseData.join(tz_id_df, Seq("__ID__"),"right").drop("__ID__","G_GEOMETRY_LOCATION_LAT","G_GEOMETRY_LOCATION_LNG").cache()
  }
  
  def checkHttpApplicationAPI(responseDs: Dataset[Response]): Unit = {
     logger.info("[+] Validating Yarn application API Http Response Request")
     val error = responseDs.where("__STATUS_CODE__ <> 200").selectExpr("to_json(struct(*)) as error")
     if (error.count > 0) {
	  logger.error(s"Unable to get the application id status")
	  val insertJobAuditQuery = s"INSERT INTO T_NABU_JOBS_AUDIT(ID, JOB_NAME, STATUS, SPARK_APPLICATION_ID,START_TIME,END_TIME,ERROR_MESSAGE) VALUES ('${jobId}','google_${job}','${ERROR}','${spark.sparkContext.applicationId}',SYSTIMESTAMP,SYSTIMESTAMP,'${error.as[String].first}')"
	  executeJdbcQuery(insertJobAuditQuery)
	  throw new Exception(error.as[String].first)
		}
  }
  
  def getApplicationStatus(applicationId: String): Option[String] = {
      val credentials = encodeBase64(s"${workload_username}:${workload_password}")
      val responseDs = almaren
			.sourceSql(s"select monotonically_increasing_id() as __ID__,'${applicationStatusAPIUrl}/${applicationId}/state' as __URL__")
		        .http(headers = Map("authorization" -> s"Basic ${credentials}"), method = "GET", readTimeout = 50000, connectTimeout = 120000)
			.cache()
			.batch
			.as[Response]

       checkHttpApplicationAPI(responseDs)
	   almaren
            .sourceDataFrame(responseDs)
            .sqlExpr("get_json_object(__BODY__,'$.state')")
            .batch
	    .as[Option[String]].first
  }
 
  def checkPreviousApplicationStatus: (String, Boolean) = {
    val applicationIdQuery = s"""select SPARK_APPLICATION_ID from T_NABU_JOBS_AUDIT where JOB_NAME = 'google_${job}' and STATUS = 'RUNNING' order by START_TIME DESC FETCH NEXT 1 ROWS ONLY"""
    val applicationIdDF = getoracleDf(applicationIdQuery).selectExpr("SPARK_APPLICATION_ID").as[Option[String]]
    applicationIdDF.isEmpty match {
	case true =>
	  logger.info(s"[+] No previous applicationId available for the job google_${job} in T_NABU_JOBS_AUDIT table")
	  logger.info(s"[+] Google job is Continuing")
	  ("No previous applicationId available", false)
	case false =>
	  val applicationId = applicationIdDF.first
	  applicationId match {
	    case Some(applicationId) =>
		val status = getApplicationStatus(applicationId) match {
		  case Some(status) => status
		  case _ =>
		    logger.info("[+] Cannot get the application id status")
		    "Cannot get the application status"
		}
		logger.info(s"[+] Previous Application status for the job google_${job} is $status")
		status match {
		   case "FINISHED" | "FAILED" | "KILLED" => (status, false)
		   case _ => (status, true)
		}
	    case None =>
		logger.info(s"[+] No previous applicationId available for the job google_${job} in T_NABU_JOBS_AUDIT table")
		logger.info(s"[+] Google job is Continuing")
		("No previous applicationId available", false)
	  }
     }
  }

  def formatToMicrosoftTimeZone(df: DataFrame):DataFrame = {
    val timeZoneMap = df.select("G_TIMEZONEID").distinct().collect().map(row => {
      val googleZoneId = row.getAs[String]("G_TIMEZONEID")
        if (googleZoneId == null || googleZoneId.isEmpty) {
          (googleZoneId, null)
        } else {
           val zoneId = ZoneId.of(googleZoneId)
           val currentTime = ZonedDateTime.now(zoneId)
           val offset = currentTime.getOffset.getId.replace("Z", "+00:00")
           val microsoftZoneName = TimeZone.getWindowsID(googleZoneId) // Example Microsoft zone name lookup
           (googleZoneId, s"(UTC$offset) $microsoftZoneName")
        }
    }).toMap
    df.withColumn("G_FORMATTED_TIMEZONE",expr(s"CASE ${timeZoneMap.map { case (k, v) =>
      if (k == null || v == null) "WHEN G_TIMEZONEID IS NULL THEN NULL"
      else s"WHEN G_TIMEZONEID = '$k' THEN '$v'"
    }.mkString(" ")} ELSE NULL END"))
  }

  def jobProcess : Unit = {
    logger.info("[+] Starting main Process")
    val mainQuery = batchMode match {
      case true => sql_query
      case false => s"""$sql_query where UPDT_TS >= TO_CHAR(TO_TIMESTAMP('${checkpoint.getOrElse(checkpointValue)}', 'YYYY-MM-DD HH24:MI:SS'), 'DD-MON-YY HH.MI.SS.FF9 AM')"""
    }
    val masterData = getoracleDf(mainQuery)
    val totalRecords = masterData.count
    logger.info(s"[+] Total records to be processed:${totalRecords}")
    val geoCodeResponseData = geoCodeData(masterData)
    logger.info("[+] Truncating the Staging Table for geo code data")
    executeJdbcQuery(s"TRUNCATE TABLE $stgTable")	
    targetJdbcData(geoCodeResponseData)
    logger.info("[+] Merging the goeCode Data into the Source Table ")	
    executeJdbcQuery(geoCodeMergeFile)
    val timeZoneSource_df = geoCodeResponseData.select("STUDY_NUM","SITE_NUM","G_GEOMETRY_LOCATION_LAT","G_GEOMETRY_LOCATION_LNG")
    val timeZoneResponseData = timeZoneData(timeZoneSource_df)
    val formattedTimeZoneDf = formatToMicrosoftTimeZone(timeZoneResponseData)
    logger.info("[+] Truncating the Staging Table for timezone data")
    executeJdbcQuery(s"TRUNCATE TABLE $stgTable")
    targetJdbcData(formattedTimeZoneDf)
    logger.info("[+] Merging the timeZone Data into the Source Table ")	
    executeJdbcQuery(timeZoneMergeFile)
	
    val newCheckpointDataset = newCheckpoint(masterData)
    updateCheckpoint(newCheckpointDataset)
  }
}
   
Try {
  googleGeoCode.checkPreviousApplicationStatus match{
    case (status, true) =>
      logger.error(s"Previous Google Job is in ${status} State")
      val insertJobAuditQuery = s"INSERT INTO T_NABU_JOBS_AUDIT(ID, JOB_NAME, STATUS, SPARK_APPLICATION_ID,START_TIME,END_TIME,ERROR_MESSAGE) VALUES ('${googleGeoCode.jobId}','google_${googleGeoCode.job}','${googleGeoCode.ERROR}','${spark.sparkContext.applicationId}',SYSTIMESTAMP,SYSTIMESTAMP,'Previous Job is $status')"
      googleGeoCode.executeJdbcQuery(insertJobAuditQuery)
      sys.exit(1)
    case (_, _) => logger.info(s"Google job is Starting")
  }
  logger.info(s"[+] Inserting into JOB AUDIT For the Google Object")
  val insertJobAuditQuery = s"INSERT INTO T_NABU_JOBS_AUDIT(ID, JOB_NAME, STATUS, SPARK_APPLICATION_ID,START_TIME) VALUES ('${googleGeoCode.jobId}','google_${googleGeoCode.job}','${googleGeoCode.RUNNING}','${spark.sparkContext.applicationId}',SYSTIMESTAMP)"
  googleGeoCode.executeJdbcQuery(insertJobAuditQuery)
  googleGeoCode.jobProcess  
}match{
  case Success(j) =>
    logger.info(s"Successfully completed Google Object")
    logger.info(s"[+] Updating JOB AUDIT with status SUCCESS")
    val updateJobAuditQuery = s"UPDATE T_NABU_JOBS_AUDIT SET STATUS='${googleGeoCode.COMPLETED}', END_TIME=SYSTIMESTAMP WHERE ID='${googleGeoCode.jobId}'"
    googleGeoCode.executeJdbcQuery(updateJobAuditQuery)
    sys.exit(0)
  case Failure(f) =>
    logger.error(s"Failed to execute google Object with Error Message : ", f)
    logger.error(s"Failed to execute google Object")
    val errorLen = f.getLocalizedMessage.length
    val error = {
      if (errorLen < 4000)
        f.getLocalizedMessage
      else
        f.getLocalizedMessage.substring(0, 4000)
    }
    logger.error(s"[+] Updating JOB AUDIT TABLE with status ERROR")
    val updateQuery = s"""UPDATE T_NABU_JOBS_AUDIT SET STATUS='${googleGeoCode.ERROR}', END_TIME=SYSTIMESTAMP,ERROR_MESSAGE ='${error.replaceAll("'", "")}' WHERE ID='${googleGeoCode.jobId}'"""
    Try {
      googleGeoCode.executeJdbcQuery(updateQuery)
    } match {
      case Success(j) =>
        logger.info(s"[+] Updating JOB AUDIT with status is completed")
      case Failure(f) =>
        logger.error(s"Failed to execute Oracle Audit Update Query with Error Message : ", f)
        logger.error(s"[+] Updating JOB AUDIT with status is failed")
        sys.exit(1)
    }
    sys.exit(1)
}
