package com.inadco.cassandra.spark.jdbc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import com.datastax.spark.connector._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.SparkConf
import akka.actor._
import akka.actor.Scheduler
import akka.actor.Scheduler
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global
import scala.tools.nsc.doc.model.Val
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext

/**
 * An spark app read and register all Cassandra tables as schema RDDs in Spark SQL and starts an embedded HiveThriftServer2 to make those tables accessible via jdbc:hive2 protocol
 * Notes:
 * - Currently only support basic/primitive data types.
 * - Cassandra table definitions are fetched every 3 seconds. A new RDD will be created under the same name of the corresponding Cassandra table is changed
 * @author hduong
 */
object InadcoCSJServer{
  private val logger = Logger.getLogger(getClass());
  def main(args: Array[String]) {
    try {
      val server = new InadcoCSJServer()
      server.init()
      server.start()
    } catch {
      case e: Exception =>
        logger.error("Error starting InadcoHiveThriftServer", e)
        System.exit(-1)
    }
  }

}

class InadcoCSJServer {
  private val logger = Logger.getLogger(getClass());
  val system = ActorSystem("System")
  val hiveTables = new scala.collection.mutable.HashMap[String, StructType]()
  val appConfig = loadConfig()
  def init() {

  }
  def loadConfig() = {

    //load all the properties files
    val defaultConf = ConfigFactory.load()
    val overrideFile = new File(System.getenv("INADCO_CSJB_HOME") + "/config/csjb-default.properties")
    if (overrideFile.exists()) {
      logger.info("Found override properties from: " + overrideFile.toString())
    } else {
      logger.info("NOT Found override properties from: " + overrideFile.toString())
      throw new RuntimeException("NOT Found override properties from: " + overrideFile.toString())
    }
    ConfigFactory.parseFile(overrideFile).withFallback(defaultConf)
  }

  def start() {
    logger.info("Starting InadcoCSJBServer.....")

    //init new spark context
    val sparkConf = new SparkConf()
    sparkConf.set("spark.scheduler.mode", "FAIR")
    sparkConf.set("spark.cores.max", appConfig.getString("spark.cores.max"))
    sparkConf.set("spark.cassandra.connection.host", appConfig.getString("spark.cassandra.connection.host"))
    sparkConf.set("spark.cassandra.auth.username", appConfig.getString("spark.cassandra.auth.username"))
    sparkConf.set("spark.cassandra.auth.password", appConfig.getString("spark.cassandra.auth.password"))
    sparkConf.set("spark.executor.memory", appConfig.getString("spark.executor.memory"))

    sparkConf.setMaster(appConfig.getString("inadco.spark.master"))
    sparkConf.setAppName(appConfig.getString("inadco.appName"))
    val sc = new SparkContext(sparkConf)

    //add handler to gracefully shutdown
    Runtime.getRuntime.addShutdownHook(
      new Thread() {
        override def run() {
          logger.info("Shutting down InadcoHiveThriftServer...")
          if (sc != null) {
            sc.stop();
          }
          logger.info("Spark context stopped.")
        }
      })

    //hive stuff
    //    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    HiveThriftServer2.startWithContext(sqlContext)

    //register all Cassandra tables		
    val startDelayMs = new FiniteDuration(0, java.util.concurrent.TimeUnit.MILLISECONDS)
    val intervalMs = new FiniteDuration(appConfig.getLong("inadco.tableList.refresh.intervalMs"), java.util.concurrent.TimeUnit.MILLISECONDS)

    val cancellable = system.scheduler.schedule(startDelayMs, intervalMs)({
      registerCassandraTables(sc, sparkConf, sqlContext)
    })

    logger.info("InadcoCSJServer started successfully")
  }
  def stop() {

  }

  def registerCassandraTables(sc: SparkContext, sparkConf: SparkConf, sqlContext: SQLContext) {
    val cassMetaDataDAO = new CassandraMetaDataDAO(sparkConf)
    val keyspaceList = cassMetaDataDAO.getKeySpaceList()
    keyspaceList.foreach { keyspace =>
      cassMetaDataDAO.getTableList(keyspace).foreach(tableName => registerCassandraTable(keyspace, tableName, cassMetaDataDAO, sc, sqlContext))
    }
  }

  def registerCassandraTable(keyspace: String, tableName: String, cassMetaDataDAO: CassandraMetaDataDAO, sc: SparkContext, sqlContext: SQLContext) {
    //format full table name with keyspace_ prefix
    val hiveTableName = keyspace + "_" + tableName
    logger.info("Try to register hive table " + hiveTableName + " ...")
    try {
      //	  		val rdd = sc.cassandraTable(keyspace, tableName)
      //	  		val colList = cassMetaDataDAO.getTableColumns(keyspace, tableName).toArray
      //	  		val hiveSchema = StructType(colList.map(colMeta => HiveSchemaUtils.createStructField(colMeta)))
      //
      //	  		val existingHiveSchema = hiveTables.get(hiveTableName)
      //	  		if(!HiveSchemaUtils.isSameSchema(existingHiveSchema, Some(hiveSchema))){
      //		  		hiveTables.put(hiveTableName, hiveSchema)
      //		  		logInfo("Created hive schema " + hiveSchema.toString)

      //broad cast column list to workers
      //		  		val cassRowUtils = sc.broadcast(new CassandraRowUtils())
      //		  		val broadCastedColList= sc.broadcast(colList)

      //		  		val rowRDD = rdd.map(
      //						row =>org.apache.spark.sql.Row.fromSeq(broadCastedColList.value.map(
      //            	colMeta =>cassRowUtils.value.extractCassandraRowValue(row, colMeta))))
      //
      //					val rowSchemaRDD = hiveContext.createDataFrame(rowRDD, hiveSchema)
      //					rowSchemaRDD.registerTempTable(hiveTableName)

      val rowSchemaRDD = sqlContext.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> tableName, "keyspace" -> keyspace, "cluster" -> "Test Cluster"))
        .load()
      rowSchemaRDD.createOrReplaceTempView(hiveTableName)

      logger.info("Registered table " + hiveTableName)
      //	  		}

    } catch {
      case e: Exception => logger.error("Failed to register table " + hiveTableName, e)
    }
  }

}



